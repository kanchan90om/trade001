require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WsClient = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// Railway / Uptime Health Check endpoint
app.get('/', (req, res) => {
  res.json({
    status: 'running',
    message: 'Dhan WebSocket Gateway is active',
    activeSubscribers: activeSubscriptions.size,
    timestamp: new Date().toISOString(),
  });
});

// ==========================================
// Credentials from Environment Variables
// ==========================================
const CLIENT_ID = process.env.DHAN_CLIENT_ID;
const ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN;

if (!CLIENT_ID || !ACCESS_TOKEN) {
  console.error('⚠️ [CRITICAL] DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN is missing in environment variables!');
}

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
      
      // डिफ़ॉल्ट: पूरा 2 साल (730 दिन) का इतिहास
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
      // ---------------- 2. INTRADAY CANDLES (1m, 5m, 15m, 60m) ----------------
      dhanEndpoint = 'https://api.dhan.co/v2/charts/intraday';

      // 1-मिनट के लिए 10 दिन, बाकी 5m/15m के लिए पहले राउंड में 30 दिन
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
        'client-id': CLIENT_ID || '',
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
  ACCESS_TOKEN || ''
)}&clientId=${encodeURIComponent(CLIENT_ID || '')}&authType=2`;

let dhanWs = null;
const activeSubscriptions = new Map();

// ------------------------------------------
// 1. Dhan WebSocket Manager
// ------------------------------------------
function connectDhan() {
  if (!CLIENT_ID || !ACCESS_TOKEN) return;

  console.log('[Dhan] Connecting to Live Feed...');
  dhanWs = new WsClient(DHAN_WS_URL, {
    headers: { 'User-Agent': 'DhanHQ-NodeClient/2.0' },
  });

  dhanWs.on('open', () => {
    console.log('[Dhan] Connected successfully.');

    dhanWs.send(
      JSON.stringify({
        RequestCode: 11,
        ClientId: CLIENT_ID,
        Token: ACCESS_TOKEN,
      })
    );

    if (activeSubscriptions.size > 0) {
      const instruments = Array.from(activeSubscriptions.keys()).map((id) => ({
        ExchangeSegment: 'NSE_EQ',
        SecurityId: id.toString(),
      }));
      sendSubscription(15, instruments);
    }
  });

  dhanWs.on('message', (data) => {
    if (typeof data === 'string' || data[0] === 0x7b) return;

    const buffer = Buffer.from(data);
    const responseCode = buffer.readUInt8(0);

    if (responseCode === 2 || responseCode === 4 || responseCode === 8) {
      const exchangeSegment = buffer.readUInt8(1);
      const securityId = buffer.readInt32LE(4);
      const ltp = buffer.readFloatLE(8);

      const tick = {
        securityId: securityId.toString(),
        segment: exchangeSegment,
        ltp: Number(ltp.toFixed(2)),
        timestamp: Date.now(),
      };

      io.to(`stock:${securityId}`).emit('tick', tick);
    }
  });

  dhanWs.on('error', (err) => {
    console.error('[Dhan Error]', err.message);
  });

  dhanWs.on('close', (code, reason) => {
    console.log(`[Dhan] Disconnected (Code: ${code}). Reconnecting in 3 seconds...`);
    setTimeout(connectDhan, 3000);
  });
}

function sendSubscription(requestCode, instrumentList) {
  if (dhanWs && dhanWs.readyState === WsClient.OPEN && instrumentList.length > 0) {
    dhanWs.send(
      JSON.stringify({
        RequestCode: requestCode,
        InstrumentCount: instrumentList.length,
        InstrumentList: instrumentList,
      })
    );
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

    const room = `stock:${securityId}`;
    socket.join(room);

    const currentCount = activeSubscriptions.get(securityId) || 0;
    activeSubscriptions.set(securityId, currentCount + 1);

    if (currentCount === 0) {
      sendSubscription(15, [{ ExchangeSegment: segment, SecurityId: securityId.toString() }]);
      console.log(`[Subscribed Dhan] SecurityId: ${securityId}`);
    }
  });

  socket.on('unsubscribe', ({ securityId, segment = 'NSE_EQ' }) => {
    if (!securityId) return;

    const room = `stock:${securityId}`;
    socket.leave(room);

    const currentCount = activeSubscriptions.get(securityId) || 0;
    if (currentCount <= 1) {
      activeSubscriptions.delete(securityId);
      sendSubscription(16, [{ ExchangeSegment: segment, SecurityId: securityId.toString() }]);
      console.log(`[Unsubscribed Dhan] SecurityId: ${securityId}`);
    } else {
      activeSubscriptions.set(securityId, currentCount - 1);
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
