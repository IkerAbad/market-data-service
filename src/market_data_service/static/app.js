"use strict";

const $ = (id) => document.getElementById(id);

const state = {
  symbol: null,
  mode: "live",
  interval: 1,
  candles: [],
  book: { bids: [], asks: [] },
  lastPrice: null,
  trades: [],
  status: null,
  ws: null,
  reconnectAttempt: 0,
  maxCandles: 180,
};

const MAX_TAPE = 36;
const INTERVAL_LABEL = { 1: "1s", 5: "5s", 60: "1m" };
const MONO = '"Cascadia Mono", "SFMono-Regular", Consolas, monospace';

async function api(path) {
  const response = await fetch(path, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    throw new Error(`${path}: HTTP ${response.status}`);
  }
  return response.json();
}

function precisionFor(value) {
  const abs = Math.abs(value);
  if (!Number.isFinite(abs) || abs === 0) return 2;
  if (abs >= 1000) return 2;
  if (abs >= 1) return 4;
  if (abs >= 0.01) return 5;
  return 8;
}

function decimalsOf(raw) {
  const text = String(raw);
  if (!text.includes(".")) return 0;
  return text.split(".")[1].replace(/0+$/, "").length;
}

function formatPrice(raw, maxDecimals = 8) {
  const value = Number(raw);
  if (!Number.isFinite(value)) return String(raw);
  const text = String(raw);
  const decimals =
    text.includes(".") || /e/i.test(text)
      ? Math.min(maxDecimals, Math.max(2, decimalsOf(text.replace(/e.*$/i, ""))))
      : Math.min(maxDecimals, precisionFor(value));
  return value.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function formatQty(raw) {
  const value = Number(raw);
  if (!Number.isFinite(value)) return String(raw);
  const decimals = Math.min(8, precisionFor(value));
  return value.toLocaleString("en-US", {
    minimumFractionDigits: Math.min(decimals, 2),
    maximumFractionDigits: decimals,
  });
}

function compact(value) {
  if (!Number.isFinite(value)) return "--";
  const abs = Math.abs(value);
  if (abs >= 1e9) return `${(value / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${(value / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${(value / 1e3).toFixed(2)}k`;
  return value.toFixed(0);
}

function formatDuration(seconds) {
  if (!Number.isFinite(seconds)) return "--";
  const total = Math.floor(seconds);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function shortTime(iso) {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "--";
  return date.toISOString().slice(11, 19);
}

function setConn(connected) {
  const dot = $("dot");
  dot.classList.remove("ok", "bad");
  dot.classList.add(connected ? "ok" : "bad");
  const label = connected ? (state.mode === "replay" ? "replay" : "live") : "reconnecting";
  $("conn-text").textContent = label;
}

function updateMode() {
  $("tape-mode").textContent = state.mode;
  if (state.ws && state.ws.readyState === WebSocket.OPEN) setConn(true);
}

function connect() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${protocol}://${window.location.host}/ws`);
  state.ws = socket;

  socket.addEventListener("open", () => {
    state.reconnectAttempt = 0;
    setConn(true);
  });

  socket.addEventListener("message", (event) => {
    let message;
    try {
      message = JSON.parse(event.data);
    } catch {
      return;
    }
    handleMessage(message);
  });

  socket.addEventListener("close", () => {
    setConn(false);
    scheduleReconnect();
  });

  socket.addEventListener("error", () => socket.close());
}

function scheduleReconnect() {
  const windowMs = Math.min(15000, 500 * 2 ** state.reconnectAttempt);
  const delay = windowMs / 2 + Math.random() * (windowMs / 2);
  state.reconnectAttempt += 1;
  window.setTimeout(connect, delay);
}

function handleMessage(message) {
  switch (message.type) {
    case "hello":
      state.symbol = message.symbol;
      state.mode = message.mode === "replay" ? "replay" : "live";
      $("symbol").textContent = message.symbol;
      updateMode();
      break;
    case "trade":
      onTrade(message);
      break;
    case "book":
      state.book = { bids: message.bids, asks: message.asks };
      renderBook();
      break;
    case "candle":
      onCandle(message);
      break;
    default:
      break;
  }
}

function onTrade(message) {
  const previous = state.lastPrice;
  const price = Number(message.price);
  if (Number.isFinite(price)) {
    state.lastPrice = price;
    const element = $("price");
    element.textContent = formatPrice(message.price);
    element.classList.remove("up", "down");
    if (previous !== null && price !== previous) {
      element.classList.add(price > previous ? "up" : "down");
      window.setTimeout(() => element.classList.remove("up", "down"), 240);
    }
    $("stat-price").textContent = formatPrice(message.price);
    updateChange();
  }

  state.trades.unshift({
    price: message.price,
    quantity: message.quantity,
    time: message.time,
    side: message.side,
  });
  if (state.trades.length > MAX_TAPE) state.trades.pop();
  prependTapeRow(state.trades[0]);
}

function onCandle(message) {
  if (message.interval !== state.interval) return;
  const candle = {
    ...message,
    open: Number(message.open),
    high: Number(message.high),
    low: Number(message.low),
    close: Number(message.close),
    volume: Number(message.volume),
    quote_volume: Number(message.quote_volume),
    t: Date.parse(message.open_time),
  };
  const index = state.candles.findIndex((item) => item.open_time === candle.open_time);
  if (index >= 0) {
    state.candles[index] = candle;
  } else {
    state.candles.push(candle);
    state.candles.sort((a, b) => a.t - b.t);
    if (state.candles.length > state.maxCandles) state.candles.shift();
  }
  draw();
  updateStats();
  updateChange();
}

async function loadCandles() {
  const interval = state.interval;
  try {
    const rows = await api(`/api/v1/candles?interval=${INTERVAL_LABEL[interval]}&limit=${state.maxCandles}`);
    if (interval !== state.interval) return;
    state.candles = rows.map((row) => ({
      ...row,
      open: Number(row.open),
      high: Number(row.high),
      low: Number(row.low),
      close: Number(row.close),
      volume: Number(row.volume),
      quote_volume: Number(row.quote_volume),
      t: Date.parse(row.open_time),
    }));
    draw();
    updateStats();
    updateChange();
  } catch (error) {
    console.warn(error);
  }
}

async function loadBook() {
  try {
    const data = await api("/api/v1/book?limit=20");
    state.book = { bids: data.bids, asks: data.asks };
    renderBook();
  } catch (error) {
    console.warn(error);
  }
}

async function loadPrice() {
  try {
    const data = await api("/api/v1/price");
    state.symbol = data.symbol;
    $("symbol").textContent = data.symbol;
    state.lastPrice = Number(data.price);
    $("price").textContent = formatPrice(data.price);
    $("stat-price").textContent = formatPrice(data.price);
  } catch (error) {
    console.warn(error);
  }
}

async function loadStatus() {
  try {
    const data = await api("/api/v1/status");
    state.status = data;
    renderStatus(data);
  } catch (error) {
    console.warn(error);
  }
}

function renderStatus(data) {
  const ingest = data.ingest;
  const storage = data.storage;
  const ingestion = $("st-ingest");
  if (state.mode === "replay") {
    ingestion.textContent = "replay";
    ingestion.className = "value good";
  } else {
    ingestion.textContent = ingest.connected ? "connected" : "reconnecting";
    ingestion.className = `value ${ingest.connected ? "good" : "bad"}`;
  }

  $("st-tradeid").textContent = ingest.last_trade_id ?? "--";
  const lag = ingest.lag_seconds;
  $("st-lag").textContent = lag === null ? "--" : `${Math.round(lag * 1000)} ms`;
  $("st-reconnects").textContent = ingest.reconnects;
  $("st-gaps").textContent = ingest.gaps_detected;
  $("st-backfilled").textContent = ingest.trades_backfilled;
  const missing = $("st-missing");
  missing.textContent = ingest.trades_missing;
  missing.className = `value ${ingest.trades_missing > 0 ? "bad" : ""}`;
  $("st-rows").textContent = compact(storage.trades_stored);
  $("st-files").textContent = storage.trades_files + storage.book_files;
  $("st-uptime").textContent = formatDuration(data.uptime_seconds);
  $("stat-lag").textContent = lag === null ? "--" : `${Math.round(lag * 1000)} ms`;
}

function cumulative(levels) {
  let total = 0;
  return levels.map((level) => {
    total += Number(level[1]);
    return total;
  });
}

function renderBook() {
  const asks = state.book.asks.slice(0, 10);
  const bids = state.book.bids.slice(0, 10);
  if (asks.length === 0 && bids.length === 0) return;

  const askTotals = cumulative(asks);
  const bidTotals = cumulative(bids);
  const maxTotal = Math.max(askTotals.at(-1) ?? 0, bidTotals.at(-1) ?? 0, 1);

  renderSide($("asks"), asks, askTotals, maxTotal, "ask", true);
  renderSide($("bids"), bids, bidTotals, maxTotal, "bid", false);

  const bestBid = Number(bids[0]?.[0]);
  const bestAsk = Number(asks[0]?.[0]);
  const spread = Number.isFinite(bestBid) && Number.isFinite(bestAsk) ? bestAsk - bestBid : null;
  if (spread === null) {
    $("spread-value").textContent = "--";
  } else {
    const decimals = Math.min(
      8,
      Math.max(2, decimalsOf(bids[0][0]), decimalsOf(asks[0][0])),
    );
    $("spread-value").textContent = spread.toLocaleString("en-US", {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
  }
  $("book-meta").textContent = "100ms";
}

function renderSide(container, levels, totals, maxTotal, side, reverse) {
  container.replaceChildren();
  const rows = levels.map((level, index) => ({
    price: level[0],
    quantity: level[1],
    total: totals[index],
  }));
  if (reverse) rows.reverse();

  for (const row of rows) {
    const element = document.createElement("div");
    element.className = `book-row ${side}`;
    const bar = document.createElement("span");
    bar.className = "bar";
    bar.style.width = `${Math.max(2, (row.total / maxTotal) * 100)}%`;
    const price = document.createElement("span");
    price.textContent = formatPrice(row.price);
    const quantity = document.createElement("span");
    quantity.textContent = formatQty(row.quantity);
    const total = document.createElement("span");
    total.textContent = formatQty(row.total);
    element.append(bar, price, quantity, total);
    container.append(element);
  }
}

function makeTapeRow(trade) {
  const row = document.createElement("div");
  row.className = `tape-row ${trade.side}`;
  const price = document.createElement("span");
  price.textContent = formatPrice(trade.price);
  const quantity = document.createElement("span");
  quantity.textContent = formatQty(trade.quantity);
  const time = document.createElement("span");
  time.textContent = shortTime(trade.time);
  row.append(price, quantity, time);
  return row;
}

function prependTapeRow(trade) {
  const container = $("tape");
  container.prepend(makeTapeRow(trade));
  while (container.children.length > 24) {
    container.lastElementChild.remove();
  }
}

function updateStats() {
  let volume = 0;
  let trades = 0;
  for (const candle of state.candles) {
    volume += Number(candle.quote_volume) || 0;
    trades += Number(candle.trade_count) || 0;
  }
  $("stat-volume").textContent = volume > 0 ? `$${compact(volume)}` : "--";
  $("stat-trades").textContent = compact(trades);
  $("chart-meta").textContent = `${state.candles.length} buckets · ${state.interval}s`;
}

function updateChange() {
  if (state.lastPrice === null || state.candles.length === 0) return;
  const reference = state.candles[0].open;
  if (!Number.isFinite(reference) || reference === 0) return;
  const change = ((state.lastPrice - reference) / reference) * 100;
  const element = $("change");
  element.textContent = `${change >= 0 ? "+" : ""}${change.toFixed(2)}%`;
  element.classList.toggle("up", change >= 0);
  element.classList.toggle("down", change < 0);
}

const canvas = $("chart");
const context = canvas.getContext("2d");
let pendingFrame = null;

function draw() {
  if (pendingFrame !== null) return;
  pendingFrame = window.requestAnimationFrame(() => {
    pendingFrame = null;
    renderChart();
  });
}

function renderChart() {
  const rect = canvas.parentElement.getBoundingClientRect();
  const width = Math.max(1, Math.floor(rect.width));
  const height = Math.max(1, Math.floor(rect.height));
  const dpr = window.devicePixelRatio || 1;
  if (canvas.width !== width * dpr || canvas.height !== height * dpr) {
    canvas.width = width * dpr;
    canvas.height = height * dpr;
  }
  context.setTransform(dpr, 0, 0, dpr, 0, 0);
  context.clearRect(0, 0, width, height);

  const candles = state.candles;
  if (candles.length === 0) {
    context.fillStyle = "rgba(123, 138, 155, 0.7)";
    context.font = `12px ${MONO}`;
    context.textAlign = "center";
    context.fillText("waiting for market data…", width / 2, height / 2);
    return;
  }

  const left = 8;
  const right = 78;
  const top = 10;
  const bottom = 24;
  const plotWidth = Math.max(1, width - left - right);
  const plotHeight = Math.max(1, height - top - bottom);
  const volumeHeight = Math.max(24, plotHeight * 0.2);
  const priceHeight = plotHeight - volumeHeight - 8;
  const priceTop = top;
  const priceBottom = top + priceHeight;
  const volumeTop = priceBottom + 8;

  let low = Infinity;
  let high = -Infinity;
  for (const candle of candles) {
    if (candle.low < low) low = candle.low;
    if (candle.high > high) high = candle.high;
  }
  if (!Number.isFinite(low) || !Number.isFinite(high)) return;
  if (high === low) {
    high += high * 0.0005 || 0.5;
    low -= low * 0.0005 || 0.5;
  }
  const padding = (high - low) * 0.08;
  high += padding;
  low -= padding;
  const priceToY = (price) => priceTop + ((high - price) / (high - low)) * priceHeight;

  const gridRows = 4;
  context.font = `10px ${MONO}`;
  context.textAlign = "left";
  context.textBaseline = "middle";
  for (let row = 0; row <= gridRows; row += 1) {
    const y = priceTop + (priceHeight / gridRows) * row;
    context.strokeStyle = "rgba(255, 255, 255, 0.045)";
    context.lineWidth = 1;
    context.beginPath();
    context.moveTo(left, y + 0.5);
    context.lineTo(left + plotWidth, y + 0.5);
    context.stroke();
    const price = high - ((high - low) / gridRows) * row;
    context.fillStyle = "rgba(123, 138, 155, 0.9)";
    context.fillText(formatPriceValue(price), left + plotWidth + 8, y);
  }

  const spacing = plotWidth / candles.length;
  const bodyWidth = Math.max(1, Math.min(14, spacing * 0.62));

  let maxVolume = 0;
  for (const candle of candles) maxVolume = Math.max(maxVolume, candle.volume);
  if (maxVolume <= 0) maxVolume = 1;

  const labelEvery = Math.max(1, Math.ceil(candles.length / 7));
  for (let index = 0; index < candles.length; index += 1) {
    const candle = candles[index];
    const x = left + index * spacing + spacing / 2;
    const openY = priceToY(candle.open);
    const closeY = priceToY(candle.close);
    const up = candle.close >= candle.open;
    const color = up ? "rgba(46, 204, 155, 0.95)" : "rgba(255, 93, 108, 0.95)";

    context.strokeStyle = color;
    context.lineWidth = 1;
    context.beginPath();
    context.moveTo(x + 0.5, priceToY(candle.high));
    context.lineTo(x + 0.5, priceToY(candle.low));
    context.stroke();

    context.fillStyle = color;
    const bodyTop = Math.min(openY, closeY);
    const bodyHeight = Math.max(1, Math.abs(closeY - openY));
    context.fillRect(x - bodyWidth / 2, bodyTop, bodyWidth, bodyHeight);

    const volumeBarHeight = (candle.volume / maxVolume) * volumeHeight;
    context.fillStyle = up ? "rgba(46, 204, 155, 0.28)" : "rgba(255, 93, 108, 0.28)";
    context.fillRect(x - bodyWidth / 2, volumeTop + volumeHeight - volumeBarHeight, bodyWidth, volumeBarHeight);

    if (index % labelEvery === 0) {
      context.fillStyle = "rgba(123, 138, 155, 0.8)";
      context.textAlign = "center";
      context.textBaseline = "top";
      const labelX = Math.min(Math.max(x, left + 26), left + plotWidth - 26);
      context.fillText(shortTime(candle.open_time), labelX, volumeTop + volumeHeight + 7);
      context.textAlign = "left";
      context.textBaseline = "middle";
    }
  }

  const last = candles[candles.length - 1];
  const lastY = priceToY(last.close);
  const up = last.close >= last.open;
  context.strokeStyle = up ? "rgba(46, 204, 155, 0.55)" : "rgba(255, 93, 108, 0.55)";
  context.setLineDash([4, 4]);
  context.beginPath();
  context.moveTo(left, lastY);
  context.lineTo(left + plotWidth, lastY);
  context.stroke();
  context.setLineDash([]);

  const tag = formatPrice(last.close);
  context.font = `11px ${MONO}`;
  const tagWidth = context.measureText(tag).width + 12;
  context.fillStyle = up ? "rgba(46, 204, 155, 0.92)" : "rgba(255, 93, 108, 0.92)";
  context.fillRect(left + plotWidth + 4, lastY - 9, Math.min(tagWidth, right - 8), 18);
  context.fillStyle = "#06121a";
  context.textAlign = "left";
  context.textBaseline = "middle";
  context.fillText(tag, left + plotWidth + 10, lastY);
}

function formatPriceValue(value) {
  const decimals = precisionFor(value);
  return value.toLocaleString("en-US", {
    minimumFractionDigits: Math.min(decimals, 4),
    maximumFractionDigits: decimals,
  });
}

function updateClock() {
  const now = new Date();
  $("clock").textContent = `${now.toISOString().slice(0, 10)} ${now.toISOString().slice(11, 19)} UTC`;
}

function wireIntervals() {
  for (const button of document.querySelectorAll("#intervals button")) {
    button.addEventListener("click", async () => {
      const interval = Number(button.dataset.interval);
      if (interval === state.interval) return;
      state.interval = interval;
      state.candles = [];
      for (const other of document.querySelectorAll("#intervals button")) {
        other.classList.toggle("active", other === button);
      }
      draw();
      await loadCandles();
    });
  }
}

async function boot() {
  wireIntervals();
  updateClock();
  window.setInterval(updateClock, 1000);
  window.setInterval(loadStatus, 5000);

  await Promise.allSettled([loadPrice(), loadBook(), loadCandles(), loadStatus()]);
  connect();

  const observer = new ResizeObserver(() => draw());
  observer.observe(canvas.parentElement);
}

boot();
