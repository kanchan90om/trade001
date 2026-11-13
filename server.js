require('dotenv').config();
const dns = require('dns');
dns.setDefaultResultOrder('ipv4first'); // Force IPv4 globally for fetch & WebSocket
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WsClient = require('ws');
const cors = require('cors');
const { authenticator } = require('otplib');

const app = express();
app.use(cors());
app.use(express.json());

// ==========================================
// Credentials from Environment Variables
// ==========================================
const CLIENT_ID = (process.env.DHAN_CLIENT_ID || '').trim();
const DHAN_PIN = (process.env.DHAN_PIN || '').trim();
const TOTP_SECRET = (process.env.DHAN_TOTP_SECRET || '').trim();

// Holds the active token in memory (falls back to env token if TOTP isn't set yet)
let activeAccessToken = (process.env.DHAN_ACCESS_TOKEN || '').trim();
let tokenExpiryTime = null;

let dhanWs = null;
let dhanConnected = false;
const activeSubscriptions = new Map();

// ==========================================
// Automatic Dhan Access Token Generator (TOTP + PIN)
// ==========================================
async function refreshDhanAccessToken(reconnectSocket = true) {
  if (!CLIENT_ID || !DHAN_PIN || !TOTP_SECRET) {
    console.warn('⚠️ [Auth] DHAN_PIN or DHAN_TOTP_SECRET missing; using static DHAN_ACCESS_TOKEN.');
    return activeAccessToken;
  }

  try {
    const totpCode = authenticator.generate(TOTP_SECRET);
    const url = `https://auth.dhan.co/app/generateAccessToken?dhanClientId=${encodeURIComponent(
      CLIENT_ID
    )}&pin=${encodeURIComponent(DHAN_PIN)}&totp=${encodeURIComponent(totpCode)}`;

    const response = await fetch(url, {
      method: 'POST',
      headers: { Accept: 'application/json' },
    });

    const data = await response.json();

    if (data && data.accessToken) {
      activeAccessToken = data.accessToken.trim();
      tokenExpiryTime = data.expiryTime || new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
      console.log(`✅ [Auth] Auto-generated fresh Dhan Access Token. Valid until: ${tokenExpiryTime}`);

      if (reconnectSocket) {
        if (dhanWs) {
          dhanWs.terminate(); // Triggers clean reconnect with new token
        } else {
          connectDhan();
        }
      }
      return activeAccessToken;
    } else {
      console.error('❌ [Auth] Failed to auto-generate token:', data);
      return activeAccessToken;
    }
  } catch (err) {
    console.error('❌ [Auth] Token generation error:', err.message);
    return activeAccessToken;
  }
}

// ==========================================
// Helpers: Segment & Instrument Normalization
// ==========================================
function normalizeSegment(seg) {
  if (!seg) return 'NSE_EQ';
  const s = String(seg).toUpperCase().trim();
  if (s === 'INDEX' || s === 'NSE_IDX' || s === 'BSE_IDX' || s === 'IDX_I') return 'IDX_I';
  if (s === 'NFO' || s === 'FNO' || s === 'NSE_FNO') return 'NSE_FNO';
  if (s === 'BFO' || s === 'BSE_FNO') return 'BSE_FNO';
  if (s === 'MCX' || s === 'MCX_COMM') return 'MCX_COMM';
  if (s === 'BSE' || s === 'BSE_EQ') return 'BSE_EQ';
  return s;
}

function resolveInstrument(segment, queryInstrument) {
  if (queryInstrument && String(queryInstrument).toUpperCase() !== 'EQUITY') {
    return String(queryInstrument).toUpperCase().trim();
  }
  if (segment === 'IDX_I') return 'INDEX';
  if (segment === 'NSE_FNO' || segment === 'BSE_FNO') return 'OPTIDX';
  if (segment === 'MCX_COMM') return 'FUTCOM';
  return 'EQUITY';
}

// Health Check Endpoint
app.get('/', (req, res) => {
  res.json({
    status: 'running',
    dhanWsConnected: dhanConnected,
    autoAuthEnabled: Boolean(CLIENT_ID && DHAN_PIN && TOTP_SECRET),
    tokenExpiresAt: tokenExpiryTime,
    activeSubscribers: activeSubscriptions.size,
    timestamp: new Date().toISOString(),
  });
});

// ==========================================
// REST Route: Historical & Intraday Data Proxy
// ==========================================
app.get('/api/charts/historical', async (req, res) => {
  try {
    const {
      securityId,
      exchangeSegment = 'NSE_EQ',
      instrument,
      interval = '1D',
      fromDate,
      toDate,
    } = req.query;

    if (!securityId) {
      return res.status(400).json({ error: 'securityId parameter is required' });
    }

    if (!activeAccessToken) {
      await refreshDhanAccessToken(false);
    }

    const cleanSegment = normalizeSegment(exchangeSegment);
    const cleanInstrument = resolveInstrument(cleanSegment, instrument);
    const includeOI =
      cleanSegment === 'NSE_FNO' || cleanSegment === 'BSE_FNO' || cleanSegment === 'MCX_COMM';

    const formatISTDate = (dateObj) =>
      new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Asia/Kolkata',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
      }).format(dateObj);

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
      parts.forEach((p) => (map[p.type] = p.value));
      return `${map.year}-${map.month}-${map.day} ${map.hour}:${map.minute}:${map.second}`;
    };

    const now = new Date();
    const isDaily = interval.toUpperCase() === '1D' || interval.toUpperCase() === 'D';

    let dhanEndpoint = '';
    let payload = {};

    if (isDaily) {
      dhanEndpoint = 'https://api.dhan.co/v2/charts/historical';
      const past2Years = new Date(now.getTime() - 730 * 24 * 60 * 60 * 1000);
      const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000);

      payload = {
        securityId: String(securityId),
        exchangeSegment: cleanSegment,
        instrument: cleanInstrument,
        expiryCode: 0,
        oi: includeOI,
        fromDate: fromDate || formatISTDate(past2Years),
        toDate: toDate || formatISTDate(tomorrow),
      };
    } else {
      dhanEndpoint = 'https://api.dhan.co/v2/charts/intraday';
      const daysBack = interval === '1' ? 10 : 30;
      const pastDays = new Date(now.getTime() - daysBack * 24 * 60 * 60 * 1000);

      payload = {
        securityId: String(securityId),
        exchangeSegment: cleanSegment,
        instrument: cleanInstrument,
        interval: String(interval),
        oi: includeOI,
        fromDate: fromDate || `${formatISTDate(pastDays)} 09:15:00`,
        toDate: toDate || formatISTDateTime(now),
      };
    }

    const executeDhanFetch = async (token) => {
      const response = await fetch(dhanEndpoint, {
        method: 'POST',
        headers: {
          'access-token': token,
          'client-id': CLIENT_ID,
          'Content-Type': 'application/json',
          Accept: 'application/json',
        },
        body: JSON.stringify(payload),
      });
      return response.json();
    };

    let data = await executeDhanFetch(activeAccessToken);

    // Auto-retry once if token expired (DH-901)
    if (data && data.errorCode === 'DH-901') {
      console.log('🔄 [Auth] Token expired during REST call. Auto-refreshing...');
      const newToken = await refreshDhanAccessToken(true);
      data = await executeDhanFetch(newToken);
    }

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
          ...(includeOI && data.open_interest ? { oi: Number(data.open_interest[idx]) } : {}),
        };
      });

      bars.sort((a, b) => a.time - b.time);
      return res.json({
        success: true,
        count: bars.length,
        interval,
        segment: cleanSegment,
        instrument: cleanInstrument,
        bars,
      });
    }

    return res.status(404).json({
      error: 'No candle data returned from Dhan',
      sentPayload: payload,
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

// ------------------------------------------
// 1. Dhan WebSocket Manager
// ------------------------------------------
function connectDhan() {
  if (!CLIENT_ID || !activeAccessToken) return;

  const wsUrl = `wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(
    activeAccessToken
  )}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`;

  console.log('[Dhan] Connecting to Live Feed...');
  dhanWs = new WsClient(wsUrl);

  dhanWs.on('unexpected-response', async (req, res) => {
    console.error(`[Dhan Handshake Rejected] HTTP ${res.statusCode} ${res.statusMessage}`);
    if (res.statusCode === 401 || res.statusCode === 403) {
      console.log('🔄 [Auth] WebSocket auth rejected. Regenerating token...');
      await refreshDhanAccessToken(false);
      dhanWs = new WsClient(wsUrl, {
    family: 4, // Strictly use IPv4
    handshakeTimeout: 15000, // 15s timeout instead of failing early
  });
    }
  });

  dhanWs.on('open', () => {
    dhanConnected = true;
    console.log('[Dhan] ✅ Connected to Live Feed (v2).');

    if (activeSubscriptions.size > 0) {
      const instruments = Array.from(activeSubscriptions.entries()).map(([id, info]) => ({
        ExchangeSegment: info.segment || 'NSE_EQ',
        SecurityId: String(id),
      }));
      sendSubscription(15, instruments);
    }
  });

  dhanWs.on('message', (data) => {
    if (typeof data === 'string' || data[0] === 0x7b) return;

    const buffer = Buffer.from(data);
    if (buffer.length < 12) return;

    const responseCode = buffer.readUInt8(0);
    if (responseCode === 2 || responseCode === 4 || responseCode === 8) {
      const exchangeSegment = buffer.readUInt8(3);
      const securityId = buffer.readInt32LE(4);
      const ltp = buffer.readFloatLE(8);

      if (!ltp || isNaN(ltp)) return;

      io.to(`stock:${securityId}`).emit('tick', {
        securityId: String(securityId),
        segment: exchangeSegment,
        ltp: Number(ltp.toFixed(2)),
        timestamp: Date.now(),
      });
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
    dhanWs.send(
      JSON.stringify({
        RequestCode: requestCode,
        InstrumentCount: instrumentList.length,
        InstrumentList: instrumentList,
      })
    );
  }
}

// ------------------------------------------
// 2. Client Manager
// ------------------------------------------
io.on('connection', (socket) => {
  socket.on('subscribe', ({ securityId, segment = 'NSE_EQ' }) => {
    if (!securityId) return;
    const idStr = String(securityId);
    const cleanSegment = normalizeSegment(segment);
    socket.join(`stock:${idStr}`);

    const existing = activeSubscriptions.get(idStr);
    if (!existing) {
      activeSubscriptions.set(idStr, { count: 1, segment: cleanSegment });
      sendSubscription(15, [{ ExchangeSegment: cleanSegment, SecurityId: idStr }]);
    } else {
      existing.count += 1;
      existing.segment = cleanSegment;
    }
  });

  socket.on('unsubscribe', ({ securityId, segment = 'NSE_EQ' }) => {
    if (!securityId) return;
    const idStr = String(securityId);
    const cleanSegment = normalizeSegment(segment);
    socket.leave(`stock:${idStr}`);

    const existing = activeSubscriptions.get(idStr);
    if (existing) {
      if (existing.count <= 1) {
        activeSubscriptions.delete(idStr);
        sendSubscription(16, [{ ExchangeSegment: existing.segment || cleanSegment, SecurityId: idStr }]);
      } else {
        existing.count -= 1;
      }
    }
  });
});

// ==========================================
// Startup & 22-Hour Automatic Token Rotation
// ==========================================
const PORT = process.env.PORT || 4000;
server.listen(PORT, '0.0.0.0', async () => {
  console.log(`🚀 Gateway Server running on port ${PORT}`);
  await refreshDhanAccessToken(false);
  connectDhan();

  // Automatically renew token every 22 hours before 24h expiry
  setInterval(() => {
    console.log('⏰ [Auth] Running scheduled 22-hour Dhan token renewal...');
    refreshDhanAccessToken(true);
  }, 22 * 60 * 60 * 1000);
});
