require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WsClient = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// ==========================================
// Credentials from Environment Variables (Trimmed)
// ==========================================
const CLIENT_ID = (process.env.DHAN_CLIENT_ID || '').trim();
const ACCESS_TOKEN = (process.env.DHAN_ACCESS_TOKEN || '').trim();

if (!CLIENT_ID || !ACCESS_TOKEN) {
  console.error('⚠️ [CRITICAL] DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN is missing in environment variables!');
}

let dhanWs = null;
let dhanConnected = false;
// Map: securityId -> { count: number, segment: string }
const activeSubscriptions = new Map();

// Railway / Uptime Health Check endpoint
app.get('/', (req, res) => {
  res.json({
    status: 'running',
    dhanWsConnected: dhanConnected,
    message: 'Dhan WebSocket Gateway is active',
    activeSubscribers: activeSubscriptions.size,
    timestamp: new Date().toISOString(),
  });
});

// ==========================================
// REST Route: Historical & Intraday Data Proxy with Infinite Scroll Support
// ==========================================
app.get('/api/charts/historical', async (req, res) => {
  try {
    const {
      securityId,
      exchangeSegment = 'NSE_EQ',
      instrument = 'EQUITY',
      interval = '1D', // '1D', '1', '5', '15', '25', '60'
      fromDate,
      toDate,
    } = req.query;

    if (!securityId) {
      return res.status(400).json({ error: 'securityId parameter is required' });
    }

    if (!ACCESS_TOKEN) {
      return res.status(500).json({ error: 'Dhan ACCESS_TOKEN is not configured on server' });
    }

    // Helper: Format date to YYYY-MM-DD in IST
    const formatISTDate = (dateObj) => {
      return new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Asia/Kolkata',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
      }).format(dateObj);
    };

    // Helper: Format date & time to YYYY-MM-DD HH:mm:ss in IST
    const formatISTDateTime = (dateObj) => {
      const parts = new Intl.DateTimeFormat('en-GB', {
        timeZone: 'Asia/Kolkata',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      }).formatToParts(dateObj);

      const map = {};
      parts.forEach((p) => {
        map[p.type] = p.value;
      });

      return `${map.year}-${map.month}-${map.day} ${map.hour}:${map.minute}:${map.second}`;
    };

    const now = new Date();
    const isDaily = interval.toUpperCase() === '1D' || interval.toUpperCase() === 'D';

    let dhanEndpoint = '';
    let payload = {};

    if (isDaily) {
      // ---------------- 1. DAILY HISTORICAL CANDLES ----------------
      dhanEndpoint = 'https://api.dhan.co/v2/charts/historical';

      // Default: 2 years (730 days) history
      const past2Years = new Date(now.getTime() - 730 * 24 * 60 * 60 * 1000);
      const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000);

      payload = {
        securityId: String(securityId),
        exchangeSegment: exchangeSegment,
        instrument: instrument,
        expiryCode: 0,
        oi: false,
        fromDate: fromDate || formatISTDate(past2Years),
        toDate: toDate || formatISTDate(tomorrow),
      };
    } else {
      // ---------------- 2. INTRADAY CANDLES (1m, 5m, 15m, 25m, 60m) ----------------
      dhanEndpoint = 'https://api.dhan.co/v2/charts/intraday';

      // 10 days for 1m, 30 days for 5m/15m/25m/60m
      const daysBack = interval === '1' ? 10 : 30;
      const pastDays = new Date(now.getTime() - daysBack * 24 * 60 * 60 * 1000);

      payload = {
        securityId: String(securityId),
        exchangeSegment: exchangeSegment,
        instrument: instrument,
        interval: String(interval),
        oi: false,
        fromDate: fromDate || `${formatISTDate(pastDays)} 09:15:00`,
        toDate: toDate || formatISTDateTime(now),
      };
    }

    const dhanRes = await fetch(dhanEndpoint, {
      method: 'POST',
      headers: {
        'access-token': ACCESS_TOKEN,
        'client-id': CLIENT_ID,
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      body: JSON.stringify(payload),
    });

    const data = await dhanRes.json();

    const timestamps = data.timestamp || data.start_Time;
    if (data && data.open && data.open.length > 0 && timestamps) {
      const bars = timestamps.map((time, idx) => {
        const epochSeconds =
          typeof time === 'number'
            ? time > 9999999999
              ? Math.floor(time / 1000)
              : time
            : Math.floor(new Date(time).getTime() / 1000);

        return {
          time: epochSeconds,
          open: Number(data.open[idx]),
          high: Number(data.high[idx]),
          low: Number(data.low[idx]),
          close: Number(data.close[idx]),
          volume: Number(data.volume ? data.volume[idx] : 0),
        };
      });

      bars.sort((a, b) => a.time - b.time);
      return res.json({ success: true, count: bars.length, interval, bars });
    }

    return res.status(404).json({
      error: 'No candle data returned from Dhan',
      details: data,
    });
  } catch (error) {
    console.error('[Historical API Error]', error.message);
    res.status(500).json({ error: error.message });
  }
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
});

const DHAN_WS_URL = `wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(
  ACCESS_TOKEN
)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`;

// ------------------------------------------
// 1. Dhan WebSocket Manager (v2 Native Auth)
// ------------------------------------------
function connectDhan() {
  if (!CLIENT_ID || !ACCESS_TOKEN) return;

  console.log('[Dhan] Connecting to Live Feed...');
  dhanWs = new WsClient(DHAN_WS_URL);

  // Capture exact HTTP status if Dhan rejects the WebSocket handshake
  dhanWs.on('unexpected-response', (req, res) => {
    let body = '';
    res.on('data', (chunk) => (body += chunk));
    res.on('end', () => {
      console.error(
        `[Dhan Handshake Rejected] HTTP ${res.statusCode} ${res.statusMessage} | Body: ${body}`
      );
    });
  });

  dhanWs.on('open', () => {
    dhanConnected = true;
    console.log('[Dhan] ✅ Connected to Live Feed (v2).');

    // Resubscribe all active symbols with their exact segments (No RequestCode 11 in v2)
    if (activeSubscriptions.size > 0) {
      const instruments = Array.from(activeSubscriptions.entries()).map(([id, info]) => ({
        ExchangeSegment: info.segment || 'NSE_EQ',
        SecurityId: String(id),
      }));
      sendSubscription(15, instruments);
    }
  });

  let tickLogCount = 0;

  dhanWs.on('message', (data) => {
    if (typeof data === 'string' || data[0] === 0x7b) {
      console.log('[Dhan Text Msg]:', data.toString());
      return;
    }

    const buffer = Buffer.from(data);
    if (buffer.length < 12) return;

    const responseCode = buffer.readUInt8(0);

    // 2 = Ticker, 4 = Quote, 8 = Full Market Depth
    if (responseCode === 2 || responseCode === 4 || responseCode === 8) {
      const exchangeSegment = buffer.readUInt8(3);
      const securityId = buffer.readInt32LE(4);
      const ltp = buffer.readFloatLE(8);

      if (!ltp || isNaN(ltp)) return;

      const tick = {
        securityId: String(securityId),
        segment: exchangeSegment,
        ltp: Number(ltp.toFixed(2)),
        timestamp: Date.now(),
      };

      if (tickLogCount % 25 === 0) {
        console.log(`[LIVE TICK] ID: ${tick.securityId} | LTP: ₹${tick.ltp}`);
      }
      tickLogCount++;

      io.to(`stock:${securityId}`).emit('tick', tick);
    } else if (responseCode === 50) {
      const disconnectCode = buffer.readInt16LE(8);
      console.error(`[Dhan] Disconnect Packet Received. Code: ${disconnectCode}`);
    }
  });

  dhanWs.on('error', (err) => {
    console.error('[Dhan WS Error]:', err.message || err);
  });

  dhanWs.on('close', (code) => {
    dhanConnected = false;
    console.log(`[Dhan] Disconnected (Code: ${code}). Reconnecting in 5 seconds...`);
    setTimeout(connectDhan, 5000);
  });
}

function sendSubscription(requestCode, instrumentList) {
  if (dhanWs && dhanWs.readyState === WsClient.OPEN && instrumentList.length > 0) {
    const payload = {
      RequestCode: requestCode,
      InstrumentCount: instrumentList.length,
      InstrumentList: instrumentList,
    };
    dhanWs.send(JSON.stringify(payload));
    console.log('[Dhan WS Sent]:', JSON.stringify(payload));
  }
}

connectDhan();

// ------------------------------------------
// 2. Client Manager
// ------------------------------------------
io.on('connection', (socket) => {
  console.log(`[Client Connected] Socket ID: ${socket.id}`);

  socket.on('subscribe', ({ securityId, segment = 'NSE_EQ' }) => {
    if (!securityId) return;
    const idStr = String(securityId);
    const room = `stock:${idStr}`;
    socket.join(room);

    const existing = activeSubscriptions.get(idStr);
    if (!existing) {
      activeSubscriptions.set(idStr, { count: 1, segment });
      sendSubscription(15, [{ ExchangeSegment: segment, SecurityId: idStr }]);
      console.log(`[Subscribed Dhan] SecurityId: ${idStr} (${segment})`);
    } else {
      existing.count += 1;
      existing.segment = segment;
    }
  });

  socket.on('unsubscribe', ({ securityId, segment = 'NSE_EQ' }) => {
    if (!securityId) return;
    const idStr = String(securityId);
    const room = `stock:${idStr}`;
    socket.leave(room);

    const existing = activeSubscriptions.get(idStr);
    if (existing) {
      if (existing.count <= 1) {
        activeSubscriptions.delete(idStr);
        sendSubscription(16, [{ ExchangeSegment: existing.segment || segment, SecurityId: idStr }]);
        console.log(`[Unsubscribed Dhan] SecurityId: ${idStr}`);
      } else {
        existing.count -= 1;
      }
    }
  });

  socket.on('disconnect', () => {
    console.log(`[Client Disconnected] Socket ID: ${socket.id}`);
  });
});

const PORT = process.env.PORT || 4000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Gateway Server running on port ${PORT}`);
});
