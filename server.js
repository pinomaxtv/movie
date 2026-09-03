/**
 * ============================================================================
 * PINOMAX MTProto Streaming, Auto-Indexer & Telegram Mini App Engine
 * Powered by Express.js, GramJS (MTProto), MongoDB Atlas, Firebase & Artplayer
 * ============================================================================
 */

import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import axios from 'axios';
import cors from 'cors';
import helmet from 'helmet';
import mongoose from 'mongoose';
import { TelegramClient, Api } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import { NewMessage } from 'telegram/events/index.js';
import bigInt from 'big-integer';
import AdmZip from 'adm-zip';
import crypto from 'crypto';
if (!globalThis.crypto) globalThis.crypto = crypto;
// Ilagay ito sa tabi ng iba pang imports sa itaas ng server.js mo
import subtitleRoutes from './routes/subtitles.js';

// Load Environment Variables
dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Initialize Express App
const app = express();
const PORT = process.env.PORT || 3000;

// Core Configuration
//const APP_URL = (process.env.APP_URL || 'https://streamixph05-pinomaxstreamv12026.hf.space').replace(/\/+$/, '');///
const APP_URL = (process.env.APP_URL || 'https://movie-production-57b9.up.railway.app').replace(/\/+$/, '');
const WORKER_URL = "https://pinomax-cache.roderickalmaras05.workers.dev";
const API_ID = parseInt(process.env.API_ID || '0', 10);
const API_HASH = (process.env.API_HASH || '').trim();
const BOT_TOKEN = (process.env.BOT_TOKEN || '').trim();
const ADMIN_ID = (process.env.ADMIN_ID || '').toString().trim();
const MONGODB_URI = process.env.MONGODB_URI || '';
const FIREBASE_DATABASE_URL = (process.env.FIREBASE_DATABASE_URL || 'https://jetmax-f3e8e-default-rtdb.firebaseio.com').replace(/\/+$/, '');
const TMDB_API_KEY = process.env.TMDB_API_KEY || '86fd55697899e8444fa3da3ddd24518d';

// Payment Configuration (GCash & PayPal)
const PAYMENT_CONFIG = {
  gcash_number: process.env.GCASH_NUMBER || '09638924040',
  gcash_name: process.env.GCASH_NAME || 'PINOMAX ADMIN',
  paypal_email: process.env.PAYPAL_EMAIL || 'payments@pinomax.com',
  donate_amount: '5'
};
// 🟢 TELEGRAM MEDIA IN-MEMORY CACHE (Para instant play at walang Telegram API lag)
const telegramMediaCache = new Map();

// ---------------------------------------------------------------------------
// 1. MONGODB DATABASE & SCHEMA (Collection: stream_files)
// ---------------------------------------------------------------------------
const streamFileSchema = new mongoose.Schema({
  file_unique_id: { type: String, index: true },
  file_id: { type: String, index: true },
  unique_id: { type: String, index: true },
  fileId: { type: String, index: true },
  message_id: { type: Number },
  messageId: { type: Number },
  chat_id: { type: String },
  chatId: { type: String },
  channelId: { type: String, default: null },
  file_name: { type: String, default: 'media.mp4' },
  fileName: { type: String, default: 'media.mp4' },
  file_size: { type: Number, default: 0 },
  fileSize: { type: Number, default: 0 },
  mime_type: { type: String, default: 'video/mp4' },
  mimeType: { type: String, default: 'video/mp4' },
  poster: { type: String, default: '' },
  year: { type: String, default: '' },
  rating: { type: String, default: '8.5' },
  overview: { type: String, default: '' },
  category: { type: String, default: 'Movie' },
  episode: { type: String, default: '' },
  uploadedBy: { type: String, default: 'Admin' },
  createdAt: { type: Date, default: Date.now, index: true }
}, {
  collection: 'stream_files',
  timestamps: true,
  strict: false
});

const inMemoryFiles = new Map();
let StreamFileModel = null;
let StreamFile = null;
let isMongoConnected = false;

if (MONGODB_URI) {
  mongoose.connect(MONGODB_URI, { serverSelectionTimeoutMS: 5000 }).then(() => {
    isMongoConnected = true;
    console.log('✅ Connected to MongoDB Atlas.');
  }).catch((err) => {
    console.warn('⚠️ MongoDB fallback to memory cache:', err.message);
  });
  StreamFileModel = mongoose.models.StreamFile || mongoose.model('StreamFile', streamFileSchema);
  StreamFile = StreamFileModel;
}

async function saveStreamFileRecord(data) {
  const primaryId = data.file_unique_id || data.file_id || data.unique_id || data.fileId;
  const normalized = {
    ...data,
    file_unique_id: primaryId,
    file_id: primaryId,
    unique_id: primaryId,
    fileId: primaryId,
    chat_id: data.chat_id || data.chatId,
    chatId: data.chatId || data.chat_id,
    message_id: data.message_id || data.messageId,
    messageId: data.messageId || data.message_id,
    file_name: data.file_name || data.fileName || 'media.mp4',
    fileName: data.fileName || data.file_name || 'media.mp4',
    file_size: data.file_size !== undefined ? data.file_size : (data.fileSize || 0),
    fileSize: data.fileSize !== undefined ? data.fileSize : (data.file_size || 0),
    mime_type: data.mime_type || data.mimeType || 'video/mp4',
    mimeType: data.mimeType || data.mime_type || 'video/mp4',
    poster: data.poster || '',
    year: data.year || '',
    rating: data.rating || '8.5',
    overview: data.overview || '',
    category: data.category || 'Movie',
    episode: data.episode || '',
    updatedAt: new Date()
  };

  inMemoryFiles.set(primaryId, normalized);
  const model = StreamFile || StreamFileModel;
  if (isMongoConnected && model) {
    try {
      return await model.findOneAndUpdate(
        { $or: [ { file_unique_id: primaryId }, { file_id: primaryId } ] },
        { $set: normalized },
        { upsert: true, new: true }
      );
    } catch (e) {
      console.error('MongoDB save error:', e.message);
    }
  }
  return normalized;
}

async function getStreamFileRecord(fileId) {
  if (!fileId) return null;
  const idStr = String(fileId).trim();
  const model = StreamFile || StreamFileModel;
  if (isMongoConnected && model) {
    try {
      const fileRecord = await model.findOne({
        $or: [
          { file_unique_id: idStr },
          { file_id: idStr },
          { unique_id: idStr },
          { fileId: idStr }
        ]
      }).lean();
      if (fileRecord) return fileRecord;
    } catch (e) {
      console.error('MongoDB find error:', e.message);
    }
  }
  return inMemoryFiles.get(idStr) || null;
}

function formatBytes(bytes, decimals = 2) {
  if (!bytes || bytes === 0) return '0 Bytes';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

// ---------------------------------------------------------------------------
// 2. GRAMJS TELEGRAM MTPROTO ENGINE (AUTO-INDEXER WITH WORKER URLs)
// ---------------------------------------------------------------------------
let tgClient = null;
let isGramJsConnected = false;

if (API_ID && API_HASH && BOT_TOKEN) {
  try {
    const stringSession = new StringSession(process.env.SESSION_STRING || "");
    tgClient = new TelegramClient(stringSession, API_ID, API_HASH, {
      connectionRetries: 10,
      retryDelay: 1000,
      autoReconnect: true,
      useIPv6: false,
      timeout: 30,
      floodSleepThreshold: 60
    });
  } catch (initErr) {
    console.warn('⚠️ TelegramClient init notice:', initErr.message);
  }
}

if (tgClient && API_ID && API_HASH && BOT_TOKEN) {
  (async () => {
    try {
      console.log('Connecting Telegram GramJS MTProto client...');
      await tgClient.start({ botAuthToken: process.env.BOT_TOKEN });
      isGramJsConnected = true;
      console.log('✅ Telegram GramJS MTProto Client successfully authenticated!');
      
      // 🟢 I-PRINT ANG SESSION STRING (Para makopya mo sa Railway)
      try {
        const savedSession = tgClient.session.save();
        if (savedSession) {
          console.log('🔑 SESSION_STRING MO (Kopyahin ito sa Railway):', savedSession);
        }
      } catch (_) {}

      // 🟢 AUTO-CONNECT SA MGA STORAGE CHANNELS
      const rawChannels = process.env.STORAGE_CHANNELS || '';
      const STORAGE_CHANNELS = rawChannels
        .split(',')
        .map(id => id.trim())
        .filter(Boolean);

      if (STORAGE_CHANNELS.length > 0) {
        console.log(`🔄 Kinokonekta ang ${STORAGE_CHANNELS.length} storage channels...`);
        for (const chId of STORAGE_CHANNELS) {
          try {
            await tgClient.getInputEntity(chId);
            console.log(`✅ Channel Connected & Cached: ${chId}`);
          } catch (err) {
            console.warn(`⚠️ Warning: Hindi ma-cache ang channel ${chId}:`, err.message);
          }
        }
      }
      // Keep Alive
      setInterval(async () => {
        if (tgClient && isGramJsConnected) {
          try { await tgClient.getMe(); } catch (e) {}
        }
      }, 40000);

    // Event Handler para sa Auto-Indexing ng Videos
tgClient.addEventHandler(async (event) => {
  try {
    const message = event.message;
    if (!message) return;

    // 🟢 1. BASAHIN ANG ADMIN IDS SA TAAS (Mababasa ng lahat)
    const ADMIN_IDS = (process.env.ADMIN_ID || '').split(',').map(id => id.trim()).filter(Boolean);

    const senderId = message.senderId ? message.senderId.toString() : '';
    const peerChannelId = message.peerId?.channelId ? message.peerId.channelId.toString() : '';
    const chatId = peerChannelId ? `-100${peerChannelId}` : (message.chatId ? message.chatId.toString() : '');
    const messageId = message.id;
    const isChannelPost = Boolean(peerChannelId);
    const isSenderAdmin = ADMIN_IDS.includes(senderId) || ADMIN_IDS.includes(chatId);
    
    // 🟢 1. COMMAND: /start (Private Chat Welcome)
    if (!isChannelPost && message.message?.startsWith('/start')) {
      if (ADMIN_IDS.length > 0 && !isSenderAdmin) {
        await tgClient.sendMessage(chatId, {
          message: `⛔ <b>Access Denied!</b>\n\nYour Telegram User ID: <code>${senderId}</code>\nAuthorized Admin ID: <code>${ADMIN_IDS.join(', ')}</code>`,
          parseMode: 'html',
        });
        return;
      }

      await tgClient.sendMessage(chatId, {
        message: 
`🚀 <b>Welcome to PINOMAX MTProto Media Streaming & Downloader Engine!</b>

⚡ <b>Bypasses the 20MB Bot API Limit</b> — Supports files up to <b>2GB</b>!

*✨ Key Features:*
• 📺 <b>HTTP 206 Partial Content Streaming</b>
• 📥 <b>Direct Attachment Downloader</b>
• 🎬 <b>Standalone Embed Video Player (with CC & Subtitles)</b>

👉 <i>Mag-forward ng movie rito o mag-upload sa Channel para mag-auto index! Gamitin ang <b>/list</b> para makita ang mga uploaded movies.</i>`,
        parseMode: 'html',
      });
      return;
    }

    // 🟢 2. COMMAND: /list (Ilista ang 10 pinakabagong uploaded movies)
    if (!isChannelPost && message.message?.startsWith('/list')) {
      try {
        const model = StreamFile || StreamFileModel;
        let files = [];
        if (isMongoConnected && model) {
          files = await model.find().sort({ createdAt: -1 }).limit(10).lean();
        } else {
          files = Array.from(inMemoryFiles.values()).slice(0, 10);
        }

        if (!files || files.length === 0) {
          await tgClient.sendMessage(chatId, {
            message: '📂 <b>Wala pang naka-save na movies sa database.</b>',
            parseMode: 'html'
          });
          return;
        }

        let listText = '🎬 <b>Recent Uploaded Movies (PINOMAX):</b>\n\n';
        files.forEach((f, idx) => {
          const fId = f.file_unique_id || f.file_id || f.unique_id;
          const embedUrl = `${WORKER_URL}/embed/${fId}`;
          const title = f.file_name || f.fileName || 'Movie';
          const size = formatBytes(f.file_size || f.fileSize || 0);
          listText += `${idx + 1}. <b>${title}</b> (${size})\n`;
          listText += `   👉 <code>${embedUrl}</code>\n\n`;
        });

        await tgClient.sendMessage(chatId, {
          message: listText,
          parseMode: 'html'
        });
      } catch (err) {
        console.error('List error:', err);
      }
      return;
    }

    // 🟢 3. MEDIA HANDLER (Video Auto-Indexer)
    if (message.media && (message.media.document || message.media.video || message.file)) {
      // Harangin ang ibang tao kapag nag-upload sa PM
      if (!isChannelPost && ADMIN_IDS.length > 0 && !isSenderAdmin) {
        return;
      }

      const doc = message.media.document || message.media;
      const file = message.file;

            let fileName = file?.name || '';
            if (!fileName && doc?.attributes) {
              const fnAttr = doc.attributes.find(a => a.className === 'DocumentAttributeFilename' || a.fileName);
              if (fnAttr && fnAttr.fileName) fileName = fnAttr.fileName;
            }
            if (!fileName) fileName = `media_${messageId}.${file?.ext || 'mp4'}`;

            const fileSize = Number(file?.size || doc?.size || 0);
            const mimeType = file?.mimeType || doc?.mimeType || 'video/mp4';
            const fileUniqueId = `${chatId.replace(/^-/, '')}_${messageId}`;
          // 🛡️ HARANGIN ANG MGA KALAT NA FILES / INTRO TEST (< 5MB)
            if (fileSize < 5 * 1024 * 1024) {
              console.log(`⏩ Skipped junk/small media (${formatBytes(fileSize)}): ${fileName}`);
              return; // Hindi ito ise-save sa database
            }

            let poster = '';
            let year = '2024';
            let rating = '8.5';
            let overview = '';
            let category = fileName.toLowerCase().includes('series') ? 'Series' : 'Movie';
            let episode = '';

            // ✅ SMART TMDB TITLE CLEANER & DUAL SEARCH
            try {
              // 1. Linisin ang mga kalat na tags, channel names, resolution, at underscores
              let cleanTitle = fileName
                .replace(/\.mp4|\.mkv|\.webm|\.avi/gi, '')
                .replace(/@\w+/g, '') // Tanggalin ang @channel_name
                .replace(/tagalog dubbed|tagdub|dubbed|pinoy|tagalog|full movie/gi, '')
                .replace(/1080p|720p|480p|hdrip|bluray|web-dl|webrip|x264|x265|hevc/gi, '')
                .replace(/[\(\)\[\]_\-\.]+/g, ' ') // Palitan ang _, -, at brackets ng space
                .trim();

              // 2. Hanapin kung may Taon (e.g. 2024, 2000, 1999)
              const yearMatch = cleanTitle.match(/\b(19\d{2}|20\d{2})\b/);
              let detectedYear = yearMatch ? yearMatch[1] : '';
              if (detectedYear) {
                cleanTitle = cleanTitle.replace(detectedYear, '').trim();
              }

              // 3. Unang Search sa TMDB (May Title + Year para eksakto)
              let searchUrl = `https://api.themoviedb.org/3/search/multi?api_key=${TMDB_API_KEY}&query=${encodeURIComponent(cleanTitle)}${detectedYear ? '&year=' + detectedYear : ''}`;
              let searchRes = await axios.get(searchUrl, { timeout: 4000 });

              // 4. Fallback Search: Kung walang nahanap, mag-search uli gamit ang Title lang
              if (!searchRes.data?.results?.length && detectedYear) {
                searchUrl = `https://api.themoviedb.org/3/search/multi?api_key=${TMDB_API_KEY}&query=${encodeURIComponent(cleanTitle)}`;
                searchRes = await axios.get(searchUrl, { timeout: 4000 });
              }

              if (searchRes.data?.results?.length > 0) {
                const resItem = searchRes.data.results[0];
                if (resItem.poster_path) {
                  poster = `https://image.tmdb.org/t/p/w500${resItem.poster_path}`;
                }
                year = (resItem.release_date || resItem.first_air_date || detectedYear || '').split('-')[0] || '2024';
                rating = resItem.vote_average ? resItem.vote_average.toFixed(1) : '8.5';
                overview = resItem.overview ? (resItem.overview.length > 120 ? resItem.overview.substring(0, 120) + '...' : resItem.overview) : '';
                category = resItem.media_type === 'tv' ? 'Series' : 'Movie';
              } else {
                year = detectedYear || '2024';
              }
            } catch (_) {}

              

            await saveStreamFileRecord({
              chat_id: chatId.toString(),
              message_id: Number(messageId),
              file_name: fileName,
              file_size: fileSize,
              mime_type: mimeType,
              poster,
              year,
              rating,
              overview,
              category,
              episode,
              unique_id: fileUniqueId,
              uploadedBy: isChannelPost ? 'Channel Storage' : (senderId ? `@${senderId}` : 'Admin')
            });
            
            // I-cache agad ang media para ready sa streaming
            telegramMediaCache.set(`${chatId}_${messageId}`, {
              media: message.media,
              size: fileSize,
              mimeType,
              timestamp: Date.now()
            });

            const streamUrl = `${WORKER_URL}/stream/${fileUniqueId}`;
            const downloadUrl = `${WORKER_URL}/download/${fileUniqueId}?file=${encodeURIComponent(fileName)}`;
            const embedUrl = `${WORKER_URL}/embed/${fileUniqueId}`;
            const sizeFormatted = formatBytes(fileSize);

            // 👉 FORMAT NA TUGMANG-TUGMA SA SCREENSHOT 1:
            const replyHtml = 
`🎬 <b>Media Successfully Indexed (2GB MTProto Engine)!</b>\n\n` +
`📁 <b>File Name:</b> ${fileName}\n` +
`📦 <b>File Size:</b> ${sizeFormatted}\n` +
`🏷️ <b>Source:</b> ${isChannelPost ? 'Channel Auto-Index' : 'Direct Upload'}\n` +
`🆔 <b>File ID:</b> ${fileUniqueId}\n\n` +
`━━━━━━━━━━━━━━━━━━━━\n` +
`📺 <b>Stream URL (HTTP 206):</b>\n` +
`${streamUrl}\n\n` +
`📥 <b>Direct Download URL:</b>\n` +
`${downloadUrl}\n\n` +
`🎬 <b>Cloudflare Cached Embed URL:</b>\n` +
`${embedUrl}\n` +
`━━━━━━━━━━━━━━━━━━━━`;

         // 1. KUNG SA STORAGE CHANNEL KA NAG-UPLOAD O NAG-FORWARD:
            if (isChannelPost) {
              await tgClient.sendMessage(chatId, { 
                message: replyHtml, 
                replyTo: Number(messageId), 
                parseMode: 'html' 
              }).catch((e) => console.error('Channel send error:', e.message));

              if (ADMIN_ID && chatId !== ADMIN_ID) {
                await tgClient.sendMessage(ADMIN_ID, { 
                  message: replyHtml, 
                  parseMode: 'html' 
                }).catch((e) => console.error('Admin PM send error:', e.message));
              }
            } 
            // 2. KUNG DIRECT FORWARD MO SA PM NG BOT:
            else {
              await tgClient.sendMessage(chatId, { 
                message: replyHtml, 
                parseMode: 'html' 
              }).catch((e) => console.error('PM send error:', e.message));
            }
          }
        } catch (err) {
          console.error('Error in event handler:', err);
        }
      }, new NewMessage({}));

    } catch (err) {
      console.warn('⚠️ GramJS Error:', err.message);
    }
  })();
}

// ---------------------------------------------------------------------------
// 3. EXPRESS MIDDLEWARE CONFIGURATION
// ---------------------------------------------------------------------------
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());
app.use(helmet({ contentSecurityPolicy: false, crossOriginEmbedderPolicy: false }));
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', subtitleRoutes);
// ---------------------------------------------------------------------------
// 4. FIREBASE VIP & PAYMENT APIS (TUGMA SA AdminDashboardActivity.java)
// ---------------------------------------------------------------------------
app.get('/api/vip/status/:uid', async (req, res) => {
  const { uid } = req.params;
  try {
    const fb = await axios.get(`${FIREBASE_DATABASE_URL}/users/${uid}.json`);
    const user = fb.data || {};
    let isVip = user.isVip || false;
    const expiry = user.vip_expiry || 0;

    if (isVip && expiry > 0 && Date.now() > expiry) {
      isVip = false;
      await axios.patch(`${FIREBASE_DATABASE_URL}/users/${uid}.json`, { isVip: false });
    }

    return res.json({
      isVip,
      vip_expiry: expiry,
      userName: user.name || user.user_name || 'User',
      active_request: user.active_request || null
    });
  } catch (e) {
    return res.json({ isVip: false });
  }
});

app.post('/api/vip/submit-request', async (req, res) => {
  const { uid, userName, planName, amount, method, refNumber } = req.body;
  if (!uid || !refNumber) return res.status(400).json({ error: 'Kulang ang data' });

  const timestamp = Date.now();
  const dateStr = new Date(timestamp).toLocaleString('en-PH', { 
    timeZone: 'Asia/Manila',
    month: 'short',
    day: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: true
  });

  const payload = {
    uid: String(uid).trim(),
    name: String(userName || 'Telegram User').trim(),
    userName: String(userName || 'Telegram User').trim(),
    plan: String(planName || 'VIP Access').trim(),
    amount: String(amount || '5').trim(),
    ref_number: String(refNumber).trim(),
    ref: String(refNumber).trim(),
    method: String(method || 'GCASH').toUpperCase().trim(),
    status: 'pending',
    date: dateStr,
    createdAt: timestamp
  };

  try {
    // 1. I-save sa user profile (active_request)
    await axios.put(`${FIREBASE_DATABASE_URL}/users/${uid}/active_request.json`, payload);

    // 2. I-save sa "requests" node (Para sa AdminDashboardActivity.java)
    const reqKey = `REQ_${uid}_${timestamp}`;
    await axios.put(`${FIREBASE_DATABASE_URL}/requests/${reqKey}.json`, payload);

    // 3. TELEGRAM ALERT SA ADMIN PM KAPAG MAY NAG-SUBMIT
    if (tgClient && ADMIN_ID && isGramJsConnected) {
      try {
        await tgClient.sendMessage(ADMIN_ID, {
          message: 
`💰 <b>BAGONG BAYAD / DONATE SUBMISSION!</b>\n\n` +
`👤 <b>User:</b> <code>${userName}</code> (ID: <code>${uid}</code>)\n` +
`👑 <b>Plan:</b> <code>${planName}</code>\n` +
`💵 <b>Amount:</b> ₱${amount}\n` +
`💳 <b>Method:</b> <code>${method}</code>\n` +
`🔢 <b>Ref No.:</b> <code>${refNumber}</code>\n\n` +
`👉 <i>Buksan ang Admin Dashboard App para i-Approve!</i>`,
          parseMode: 'html'
        });
      } catch (_) {}
    }

    return res.json({ success: true, message: 'Naisumite sa Admin!' });
  } catch (e) {
    return res.status(500).json({ error: 'Error sa database' });
  }
});

app.get('/api/payment-config', (req, res) => {
  res.json(PAYMENT_CONFIG);
});

app.get('/stream/:fileId', async (req, res) => {
  const fileId = req.params.fileId;
  const rec = await getStreamFileRecord(fileId);
  
  if (!rec) {
    console.error(`❌ Stream Failed: File record not found in MongoDB for ID: ${fileId}`);
    return res.redirect('https://vjs.zencdn.net/v/oceans.mp4');
  }

  if (!tgClient || !isGramJsConnected) {
    console.error('❌ Stream Failed: Telegram MTProto Client not connected');
    return res.redirect('https://vjs.zencdn.net/v/oceans.mp4');
  }

  try {
    let peer = rec.chat_id || rec.chatId;
    
    // 🟢 RESOLVE TELEGRAM PEER PARA SA LAHAT NG STORAGE CHANNELS
    let targetEntity;
    try {
      targetEntity = await tgClient.getInputEntity(peer);
    } catch (resolveErr) {
      if (typeof peer === 'string' && peer.startsWith('-100')) {
        peer = bigInt(peer.replace('-100', ''));
      }
      targetEntity = await tgClient.getInputEntity(peer);
    }

    const msgIdNum = Number(rec.message_id || rec.messageId);
    const cacheKey = `${rec.chat_id}_${msgIdNum}`;
    let mediaObj = null;
    let totalSize = Number(rec.file_size || rec.fileSize || 0);

    // 🟢 1. FAST MEMORY CACHE
    if (telegramMediaCache.has(cacheKey)) {
      const cached = telegramMediaCache.get(cacheKey);
      mediaObj = cached.media;
      if (cached.size) totalSize = cached.size;
    } else {
      const msgs = await tgClient.getMessages(targetEntity, { ids: [msgIdNum] });
      const msg = msgs && msgs[0];
      
      if (!msg || !msg.media) {
        console.error(`❌ Stream Failed: Message not found in Telegram (Channel: ${peer}, Msg ID: ${msgIdNum})`);
        return res.redirect('https://vjs.zencdn.net/v/oceans.mp4');
      }
      
      mediaObj = msg.media;
      totalSize = Number(msg.file?.size || totalSize);

      telegramMediaCache.set(cacheKey, {
        media: mediaObj,
        size: totalSize,
        mimeType: rec.mime_type || 'video/mp4',
        timestamp: Date.now()
      });
    }

    const range = req.headers.range;
    let start = 0;
    let end = totalSize - 1;

    const cacheHeaders = {
      'Cache-Control': 'public, max-age=2592000, s-maxage=2592000, immutable',
      'CDN-Cache-Control': 'max-age=2592000',
      'Cloudflare-CDN-Cache-Control': 'max-age=2592000',
      'Access-Control-Allow-Origin': '*',
      'Accept-Ranges': 'bytes'
    };

    // 🟢 2. STANDARD HTTP 206
    if (range) {
      const parts = range.replace(/bytes=/, '').split('-');
      start = parseInt(parts[0], 10);
      if (parts[1]) {
        end = parseInt(parts[1], 10);
      }
      res.writeHead(206, {
        ...cacheHeaders,
        'Content-Range': `bytes ${start}-${end}/${totalSize}`,
        'Content-Length': (end - start) + 1,
        'Content-Type': rec.mime_type || 'video/mp4'
      });
    } else {
      res.writeHead(200, {
        ...cacheHeaders,
        'Content-Length': totalSize,
        'Content-Type': rec.mime_type || 'video/mp4'
      });
    }

    // 🟢 3. STREAMING CHUNKS (1MB CHUNK)
    const stream = tgClient.iterDownload({
      file: mediaObj,
      offset: bigInt(start),
      limit: (end - start) + 1,
      requestSize: 1024 * 1024
    });

    let closed = false;
    req.on('close', () => { 
      closed = true;
    });

    for await (const chunk of stream) {
      if (closed) break;
      res.write(chunk);
    }
    if (!closed) res.end();

  } catch (e) {
    console.error('❌ Stream Route Critical Error:', e.message);
    if (!res.headersSent) res.redirect('https://vjs.zencdn.net/v/oceans.mp4');
  }
});

// DIRECT ATTACHMENT DOWNLOADER (VIP ONLY)
app.get('/download/:fileId', async (req, res) => {
  const fileId = req.params.fileId;
  
  // ⛔ HARANGIN ANG DIRECT LINK KUNG HINDI VIP:
  const isVip = req.query.vip === '1';
  if (!isVip) {
    return res.status(403).send('<h1 style="color:red;text-align:center;margin-top:50px;font-family:sans-serif;">⛔ 403 Forbidden: VIP Only Download Feature</h1>');
  }

  const rec = await getStreamFileRecord(fileId);
  if (!rec || !tgClient || !isGramJsConnected) {
    return res.redirect('https://vjs.zencdn.net/v/oceans.mp4');
  }

  try {
    let peer = rec.chat_id;
    if (typeof peer === 'string' && peer.startsWith('-100')) {
      peer = bigInt(peer.replace('-100', ''));
    }
    const msgs = await tgClient.getMessages(peer, { ids: [Number(rec.message_id)] });
    const msg = msgs && msgs[0];
    if (!msg || !msg.media) return res.redirect('https://vjs.zencdn.net/v/oceans.mp4');

    const totalSize = Number(msg.file?.size || rec.file_size || 0);
    const fileName = rec.file_name || rec.fileName || `media_${fileId}.mp4`;
    const cleanFileName = encodeURIComponent(fileName);

    res.writeHead(200, {
      'Content-Length': totalSize,
      'Content-Type': 'application/octet-stream',
      'Content-Disposition': `attachment; filename="${cleanFileName}"; filename*=UTF-8''${cleanFileName}`,
      'Access-Control-Allow-Origin': '*'
    });

    const stream = tgClient.iterDownload({
      file: msg.media,
      offset: bigInt(0),
      limit: totalSize,
      requestSize: 1024 * 1024
    });

    let closed = false;
    req.on('close', () => { closed = true; });
    for await (const chunk of stream) {
      if (closed) break;
      res.write(chunk);
    }
    if (!closed) res.end();
  } catch (e) {
    if (!res.headersSent) res.status(500).send('Download Error: ' + e.message);
  }
});

app.get('/embed/:fileId', async (req, res) => {
  const { fileId } = req.params;
  const rec = await getStreamFileRecord(fileId) || { file_name: 'Pinomax Video' };
  const streamUrl = `${WORKER_URL}/stream/${fileId}`;
  const downloadUrl = `${WORKER_URL}/download/${fileId}`;
  const movieTitle = rec.file_name || rec.fileName || 'Movie';

  res.render('embed_player', {
    streamUrl: streamUrl,
    downloadUrl: downloadUrl,
    title: movieTitle,
    poster: rec.poster || ''
  });
});

// ---------------------------------------------------------------------------
// 7. CATALOG ROUTE (PARA SA MINI APP & WEB)
// ---------------------------------------------------------------------------
app.get(['/', '/newapp', '/pinomax'], async (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');

  try {
    let movies = [];
    const model = StreamFile || StreamFileModel;
    if (isMongoConnected && model) {
      movies = await model.find().sort({ createdAt: -1 }).limit(100).lean();
    } else {
      movies = Array.from(inMemoryFiles.values());
    }

    const q = (req.query.q || '').trim().toLowerCase();
    let filtered = movies;
    if (q) {
      filtered = movies.filter(m => (m.file_name || m.fileName || '').toLowerCase().includes(q));
    }

    res.render('index', {
      movies: filtered.map(m => ({
        id: m.file_unique_id || m.file_id || m.unique_id,
        title: m.file_name || m.fileName || 'Movie',
        image: m.poster || '',
        rating: m.rating || '8.5',
        year: m.year || '2024',
        genre: m.category || 'Movie',
        size: formatBytes(m.file_size || m.fileSize || 0)
      })),
      totalCount: movies.length,
      searchQuery: req.query.q || '',
      paymentConfig: PAYMENT_CONFIG
    });
  } catch (err) {
    res.render('index', { movies: [], totalCount: 0, searchQuery: '', paymentConfig: PAYMENT_CONFIG });
  }
});

// ---------------------------------------------------------------------------
// 8. SUBTITLE SCRAPER & IN-MEMORY UNZIPPER (SUBDL)
// ---------------------------------------------------------------------------
app.get('/api/subtitles/:query', async (req, res) => {
  let { query } = req.params;
  if (!query || query === 'undefined') {
    return res.json({ success: false, subtitles: [] });
  }

  try {
    let tmdbId = query;

    if (isNaN(Number(query))) {
      const cleanTitle = query
        .replace(/\.mp4|\.mkv|\.webm/gi, '')
        .replace(/tagalog dubbed|tagdub|dubbed|1080p|720p|hd/gi, '')
        .replace(/[\(\)\[\]]/g, '')
        .trim();

      const searchUrl = `https://api.themoviedb.org/3/search/movie?api_key=${TMDB_API_KEY}&query=${encodeURIComponent(cleanTitle)}`;
      const searchRes = await axios.get(searchUrl, { timeout: 6000 });
      if (searchRes.data?.results?.length > 0) {
        tmdbId = searchRes.data.results[0].id;
      } else {
        return res.json({ success: false, subtitles: [] });
      }
    }

    const subdlKey = "subdl_NMbBKczp6Un1tAIRELW0fR0F2lLPbCFbsDxn0MwUK8Q";
    const subdlUrl = `https://api.subdl.com/api/v1/subtitles?api_key=${subdlKey}&tmdb_id=${tmdbId}&languages=tl,en`;

    const subRes = await axios.get(subdlUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K)' },
      timeout: 8000
    });

    const subtitlesData = subRes.data?.subtitles || [];
    const formatted = subtitlesData.map(sub => {
      const isTag = (sub.lang || sub.language || '').toLowerCase().includes('tag') || (sub.lang || '').toLowerCase().includes('tl') || (sub.lang || '').toLowerCase().includes('fil');
      const rawUrl = sub.url.startsWith('/') ? `https://dl.subdl.com${sub.url}` : sub.url;
      const release = sub.release_name ? sub.release_name.substring(0, 16) : 'HD';

      return {
        name: isTag ? `🇵🇭 Tagalog (${release})` : `🇬🇧 English (${release})`,
        url: `/api/sub-content?url=${encodeURIComponent(rawUrl)}`,
        type: 'vtt'
      };
    });

    res.json({ success: formatted.length > 0, subtitles: formatted, tmdbId });
  } catch (err) {
    res.json({ success: false, subtitles: [] });
  }
});

app.get('/api/sub-content', async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).send('URL required');

  try {
    const response = await axios.get(targetUrl, {
      responseType: 'arraybuffer',
      headers: { 
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': '*/*'
      },
      maxRedirects: 5,
      timeout: 12000
    });

    let srtText = '';
    const buffer = Buffer.from(response.data);

    // 1. KUNG ZIP FILE ANG SUBTITLE:
    if (targetUrl.toLowerCase().includes('.zip') || (buffer[0] === 0x50 && buffer[1] === 0x4B)) {
      const zip = new AdmZip(buffer);
      const zipEntries = zip.getEntries();
      
      // 🛡️ Alisin ang macOS hidden files at kunin ang totoong .srt o .vtt:
      const validEntries = zipEntries.filter(e => !e.isDirectory && !e.entryName.includes('__MACOSX'));
      const srtEntry = validEntries.find(e => e.entryName.toLowerCase().endsWith('.srt') || e.entryName.toLowerCase().endsWith('.vtt')) || validEntries[0];

      if (srtEntry) {
        srtText = srtEntry.getData().toString('utf8');
      } else {
        return res.status(404).send('No valid subtitle found inside zip');
      }
    } else {
      srtText = buffer.toString('utf8');
    }

    // 2. LINISIN ANG BOM AT I-CONVERT SA WEBVTT:
    srtText = srtText.replace(/^\uFEFF/, '').trim(); // Tanggalin ang invisible BOM error

    if (!srtText.startsWith('WEBVTT')) {
      // Palitan ang comma (,) ng tuldok (.) para sa tamang VTT timestamp
      srtText = 'WEBVTT\n\n' + srtText.replace(/(\d{2}:\d{2}:\d{2}),(\d{3})/g, '$1.$2');
    }

    res.writeHead(200, {
      'Content-Type': 'text/vtt; charset=utf-8',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': '*',
      'Cache-Control': 'public, max-age=86400'
    });
    res.end(srtText);
  } catch (err) {
    console.error('Subtitle parse error:', err.message);
    res.status(500).send('Failed to process subtitle');
  }
});

// ---------------------------------------------------------------------------
// 9. RENDER KEEP-ALIVE PINGER
// ---------------------------------------------------------------------------
app.get('/ping', (req, res) => {
  res.status(200).json({ status: 'OK', uptime: process.uptime() });
});

setInterval(async () => {
  try {
    await axios.get(`${APP_URL}/ping`, { timeout: 8000 });
  } catch (_) {}
}, 10 * 60 * 1000);

// ---------------------------------------------------------------------------
// 🛠️ 1-CLICK POSTER & RESOLUTION AUTO-FIXER PARA SA LAHAT NG LUMANG MOVIES
// ---------------------------------------------------------------------------
app.get('/api/admin/fix-posters', async (req, res) => {
  try {
    const model = StreamFile || StreamFileModel;
    if (!isMongoConnected || !model) {
      return res.status(500).send('Database not connected.');
    }

    const allFiles = await model.find();
    let updatedCount = 0;

    for (const file of allFiles) {
      const fileName = file.file_name || file.fileName || '';
      if (!fileName) continue;

      // 1. I-detect ang Resolution (4K, 1080p, 720p, 480p)
      let quality = '1080P FHD';
      if (/4k|2160p|uhd/i.test(fileName)) quality = '4K UHD';
      else if (/1080p|fhd/i.test(fileName)) quality = '1080P FHD';
      else if (/720p|hd/i.test(fileName)) quality = '720P HD';
      else if (/480p|sd/i.test(fileName)) quality = '480P SD';

      // 2. Linisin ang Title mula sa kalat at resolution
      let cleanTitle = fileName
        .replace(/\.mp4|\.mkv|\.webm|\.avi/gi, '')
        .replace(/@\w+/g, '')
        .replace(/4k|2160p|1080p|720p|480p|uhd|fhd|hdrip|bluray|web-dl|webrip|x264|x265|hevc/gi, '')
        .replace(/tagalog dubbed|tagdub|dubbed|pinoy|tagalog|full movie/gi, '')
        .replace(/[\(\)\[\]_\-\.]+/g, ' ')
        .trim();

      const yearMatch = cleanTitle.match(/\b(19\d{2}|20\d{2})\b/);
      let detectedYear = yearMatch ? yearMatch[1] : '';
      if (detectedYear) {
        cleanTitle = cleanTitle.replace(detectedYear, '').trim();
      }

      // 3. Search sa TMDB
      let poster = file.poster || '';
      let rating = file.rating || '8.5';
      let year = file.year || detectedYear || '2024';
      let overview = file.overview || '';

      try {
        let searchUrl = `https://api.themoviedb.org/3/search/multi?api_key=${TMDB_API_KEY}&query=${encodeURIComponent(cleanTitle)}${detectedYear ? '&year=' + detectedYear : ''}`;
        let searchRes = await axios.get(searchUrl, { timeout: 4000 });

        if (!searchRes.data?.results?.length && detectedYear) {
          searchUrl = `https://api.themoviedb.org/3/search/multi?api_key=${TMDB_API_KEY}&query=${encodeURIComponent(cleanTitle)}`;
          searchRes = await axios.get(searchUrl, { timeout: 4000 });
        }

        if (searchRes.data?.results?.length > 0) {
          const resItem = searchRes.data.results[0];
          if (resItem.poster_path) {
            poster = `https://image.tmdb.org/t/p/w500${resItem.poster_path}`;
          }
          year = (resItem.release_date || resItem.first_air_date || detectedYear || '').split('-')[0] || year;
          rating = resItem.vote_average ? resItem.vote_average.toFixed(1) : rating;
          overview = resItem.overview ? (resItem.overview.length > 120 ? resItem.overview.substring(0, 120) + '...' : resItem.overview) : overview;
        }
      } catch (_) {}

      // 4. I-save pabalik sa MongoDB Atlas
      await model.updateOne(
        { _id: file._id },
        { 
          $set: { 
            poster, 
            rating, 
            year, 
            overview,
            episode: quality // I-save ang resolution badge
          } 
        }
      );
      updatedCount++;
    }

    res.send(`<h1>✅ Tagumpay! Na-update ang ${updatedCount} na pelikula!</h1><p>Bumalik sa <a href="/">Homepage</a> para makita ang mga bagong HD posters.</p>`);
  } catch (err) {
    res.status(500).send('Auto-Fix Error: ' + err.message);
  }
});

// ===========================================================================
// 🛡️ SECRET ADMIN PANEL & CRUD API
// ===========================================================================

// 🟢 1. API: Kunin ang listahan ng lahat ng movies
app.get('/api/admin/all-movies', async (req, res) => {
  try {
    const model = StreamFile || StreamFileModel;
    let movies = [];
    if (mongoose.connection.readyState === 1 && model) {
      movies = await model.find().sort({ createdAt: -1 }).maxTimeMS(5000).lean();
    } else {
      movies = Array.from(inMemoryFiles.values());
    }
    return res.json({ success: true, movies: movies || [] });
  } catch (err) {
    console.error('Admin API error:', err);
    return res.json({ success: true, movies: Array.from(inMemoryFiles.values()) });
  }
});

// 🟢 2. API: I-save ang edited movie details
app.post('/api/admin/update-movie', async (req, res) => {
  try {
    const { id, file_name, poster, year, rating, category, overview, episode } = req.body;
    if (!id) return res.status(400).json({ error: 'Kulang ang ID' });

    const model = StreamFile || StreamFileModel;
    const updateData = {
      file_name,
      fileName: file_name,
      poster,
      year,
      rating,
      category,
      overview,
      episode
    };

    if (mongoose.connection.readyState === 1 && model) {
      await model.findOneAndUpdate(
        { $or: [{ file_unique_id: id }, { file_id: id }, { unique_id: id }] },
        { $set: updateData }
      );
    }

    if (inMemoryFiles.has(id)) {
      const existing = inMemoryFiles.get(id);
      inMemoryFiles.set(id, { ...existing, ...updateData });
    }

    return res.json({ success: true, message: 'Na-update nang matagumpay!' });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

// 🟢 3. API: Mag-delete ng Movie
app.delete('/api/admin/delete-movie/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const model = StreamFile || StreamFileModel;

    if (mongoose.connection.readyState === 1 && model) {
      await model.deleteOne({ 
        $or: [{ file_unique_id: id }, { file_id: id }, { unique_id: id }] 
      });
    }
    inMemoryFiles.delete(id);

    return res.json({ success: true, message: 'Movie deleted!' });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

// 🟢 4. SECRET ADMIN DASHBOARD WEB UI
app.get('/secret-admin-portal', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="tl">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>PINOMAX Admin Manager</title>
  <style>
    :root {
      --bg: #0c0d14;
      --card-bg: #161824;
      --accent: #e50914;
      --text: #f0f2f5;
      --text-muted: #8c93a8;
      --border: #23273a;
      --input-bg: #090a0f;
    }
    * { margin:0; padding:0; box-sizing:border-box; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 16px; }

    .header {
      display: flex; justify-content: space-between; align-items: center;
      flex-wrap: wrap; gap: 12px; margin-bottom: 20px;
      padding-bottom: 12px; border-bottom: 1px solid var(--border);
    }
    .search-box {
      padding: 10px 14px; background: var(--card-bg); border: 1px solid var(--border);
      border-radius: 8px; color: #fff; width: 260px; max-width: 100%; outline: none; font-size: 13px;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
      gap: 14px;
    }
    .movie-card {
      background: var(--card-bg); border-radius: 10px; border: 1px solid var(--border);
      overflow: hidden; display: flex; flex-direction: column;
    }
    .card-top { display: flex; gap: 12px; padding: 12px; }
    .card-poster {
      width: 70px; height: 100px; border-radius: 6px; object-fit: cover;
      background: #222; flex-shrink: 0;
    }
    .card-info { flex: 1; overflow: hidden; }
    .card-title { font-size: 13px; font-weight: bold; margin-bottom: 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color:#fff; }
    .card-id { font-size: 10px; font-family: monospace; color: #ff7b7b; margin-bottom: 4px; }
    .badge {
      display: inline-block; font-size: 9px; padding: 2px 6px; border-radius: 4px;
      background: #2a2e45; color: #8cb4ff; margin-right: 4px; font-weight: 600;
    }
    .badge-star { background: #3d3000; color: #ffd700; }
    .card-overview {
      font-size: 11px; color: var(--text-muted); margin-top: 6px;
      display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
    }
    .card-actions {
      display: flex; border-top: 1px solid var(--border); background: rgba(0,0,0,0.25);
    }
    .card-actions button {
      flex: 1; padding: 10px; background: transparent; border: none;
      color: #fff; font-size: 12px; font-weight: bold; cursor: pointer;
    }
    .btn-edit { color: #58a6ff !important; border-right: 1px solid var(--border); }
    .btn-delete { color: #f85149 !important; }

    #editModal {
      position: fixed; inset: 0; background: rgba(0,0,0,0.85);
      display: none; align-items: center; justify-content: center; z-index: 1000; padding: 15px;
    }
    .modal-content {
      background: var(--card-bg); border-radius: 12px; width: 100%; max-width: 480px;
      max-height: 90vh; overflow-y: auto; padding: 20px; border: 1px solid var(--border);
    }
    .form-group { margin-bottom: 10px; }
    .form-group label { display: block; font-size: 11px; color: var(--text-muted); margin-bottom: 3px; }
    .form-group input, .form-group textarea, .form-group select {
      width: 100%; padding: 8px 10px; background: var(--input-bg); border: 1px solid var(--border);
      border-radius: 6px; color: #fff; font-size: 12px; outline: none;
    }
    .modal-buttons { display: flex; gap: 10px; margin-top: 15px; }
    .modal-buttons button { flex: 1; padding: 10px; border-radius: 6px; border: none; font-weight: bold; cursor: pointer; }
    .btn-save { background: var(--accent); color: #fff; }
    .btn-cancel { background: #30363d; color: #fff; }
  </style>
</head>
<body>

  <div class="header">
    <div>
      <h1 style="font-size:20px; font-weight:900; color:var(--accent);">🎬 PINOMAX Media Manager</h1>
      <span id="totalBadge" style="font-size:12px; color:var(--text-muted);">Kinukuha ang database...</span>
    </div>
    <div style="display:flex; gap:8px;">
      <input type="text" class="search-box" id="searchInput" placeholder="🔍 Maghanap..." oninput="renderMovies()" />
      <button onclick="loadMovies()" style="padding:0 12px; background:#21262d; border:1px solid var(--border); color:#fff; border-radius:8px; cursor:pointer; font-size:12px;">🔄 Refresh</button>
    </div>
  </div>

  <div class="grid" id="moviesContainer"></div>

  <!-- EDIT MODAL -->
  <div id="editModal">
    <div class="modal-content">
      <h3 style="margin-bottom:12px; color:var(--accent); font-size:16px;">✏️ Edit Movie</h3>
      
      <div class="form-group">
        <label>File ID:</label>
        <input type="text" id="editId" disabled style="opacity:0.5;" />
      </div>

      <div class="form-group">
        <label>Title / File Name:</label>
        <div style="display:flex; gap:6px;">
          <input type="text" id="editTitle" />
          <button type="button" onclick="fetchTMDB()" style="padding:0 10px; background:#238636; border:none; border-radius:6px; color:#fff; font-size:11px; cursor:pointer;">TMDB</button>
        </div>
      </div>

      <div class="form-group">
        <label>Poster URL:</label>
        <input type="text" id="editPoster" />
      </div>

      <div style="display:flex; gap:8px;">
        <div class="form-group" style="flex:1;">
          <label>Year:</label>
          <input type="text" id="editYear" />
        </div>
        <div class="form-group" style="flex:1;">
          <label>Rating:</label>
          <input type="text" id="editRating" />
        </div>
      </div>

      <div class="form-group">
        <label>Category:</label>
        <select id="editCategory">
          <option value="Movie">Movie</option>
          <option value="Series">Series</option>
        </select>
      </div>

      <div class="form-group">
        <label>Overview:</label>
        <textarea id="editOverview" rows="3"></textarea>
      </div>

      <div class="modal-buttons">
        <button class="btn-cancel" onclick="closeModal()">Kansela</button>
        <button class="btn-save" onclick="saveMovie()">I-save</button>
      </div>
    </div>
  </div>

  <script>
    var allMovies = [];

    window.addEventListener('DOMContentLoaded', loadMovies);

    function loadMovies() {
      document.getElementById('totalBadge').innerText = 'Kinukuha ang mga pelikula...';
      fetch('/api/admin/all-movies')
        .then(function(res) { return res.json(); })
        .then(function(data) {
          allMovies = data.movies || [];
          document.getElementById('totalBadge').innerText = 'Kabuuang Pelikula: ' + allMovies.length;
          renderMovies();
        })
        .catch(function(err) {
          document.getElementById('totalBadge').innerText = 'Error: Hindi ma-load ang database';
        });
    }

    function renderMovies() {
      var q = (document.getElementById('searchInput').value || '').toLowerCase().trim();
      var container = document.getElementById('moviesContainer');
      container.innerHTML = '';

      var filtered = allMovies.filter(function(m) {
        var name = (m.file_name || m.fileName || '').toLowerCase();
        var id = (m.file_unique_id || m.file_id || m.unique_id || '').toLowerCase();
        return name.indexOf(q) !== -1 || id.indexOf(q) !== -1;
      });

      if (filtered.length === 0) {
        container.innerHTML = '<div style="color:var(--text-muted); grid-column: 1/-1; text-align:center; padding:30px;">Walang pelikula.</div>';
        return;
      }

      filtered.forEach(function(m) {
        var id = m.file_unique_id || m.file_id || m.unique_id || m._id;
        var title = m.file_name || m.fileName || 'Untitled';
        var poster = m.poster || 'https://via.placeholder.com/150x225/111/fff?text=NO+POSTER';
        var year = m.year || '2024';
        var rating = m.rating || '8.5';
        var overview = m.overview || 'Walang synopsis.';

        var card = document.createElement('div');
        card.className = 'movie-card';
        card.innerHTML = 
          '<div class="card-top">' +
            '<img class="card-poster" src="' + poster + '" onerror="this.src=\\'https://via.placeholder.com/150x225/111/fff?text=NO+POSTER\\'" />' +
            '<div class="card-info">' +
              '<div class="card-title">' + title + '</div>' +
              '<div class="card-id">ID: ' + id + '</div>' +
              '<div>' +
                '<span class="badge badge-star">⭐ ' + rating + '</span>' +
                '<span class="badge">' + year + '</span>' +
              '</div>' +
              '<div class="card-overview">' + overview + '</div>' +
            '</div>' +
          '</div>' +
          '<div class="card-actions">' +
            '<button class="btn-edit" onclick="openEdit(\\'' + encodeURIComponent(id) + '\\')">✏️ Edit</button>' +
            '<button class="btn-delete" onclick="deleteItem(\\'' + encodeURIComponent(id) + '\\')">🗑️ Delete</button>' +
          '</div>';
        container.appendChild(card);
      });
    }

    function openEdit(encId) {
      var id = decodeURIComponent(encId);
      var m = allMovies.find(function(item) {
        return (item.file_unique_id || item.file_id || item.unique_id || item._id) === id;
      });
      if (!m) return;

      document.getElementById('editId').value = id;
      document.getElementById('editTitle').value = m.file_name || m.fileName || '';
      document.getElementById('editPoster').value = m.poster || '';
      document.getElementById('editYear').value = m.year || '2024';
      document.getElementById('editRating').value = m.rating || '8.5';
      document.getElementById('editCategory').value = m.category || 'Movie';
      document.getElementById('editOverview').value = m.overview || '';

      document.getElementById('editModal').style.display = 'flex';
    }

    function closeModal() {
      document.getElementById('editModal').style.display = 'none';
    }

    function fetchTMDB() {
      var title = document.getElementById('editTitle').value;
      if (!title) return alert('Lagyan ng Title!');
      
      var clean = title.replace(/\\.[a-zA-Z0-9]+$/g, '').replace(/@\\w+/g, '').replace(/[\\(\\)\\[\\]_\\-\\.]+/g, ' ').trim();
      var yearMatch = clean.match(/\\b(19\\d{2}|20\\d{2})\\b/);
      var yr = yearMatch ? yearMatch[1] : '';
      if (yr) clean = clean.replace(yr, '').trim();

      fetch('https://api.themoviedb.org/3/search/multi?api_key=86fd55697899e8444fa3da3ddd24518d&query=' + encodeURIComponent(clean) + (yr ? '&year=' + yr : ''))
        .then(function(r) { return r.json(); })
        .then(function(d) {
          if (d.results && d.results.length > 0) {
            var item = d.results[0];
            if (item.poster_path) document.getElementById('editPoster').value = 'https://image.tmdb.org/t/p/w500' + item.poster_path;
            document.getElementById('editYear').value = (item.release_date || item.first_air_date || yr || '2024').split('-')[0];
            document.getElementById('editRating').value = item.vote_average ? item.vote_average.toFixed(1) : '8.5';
            document.getElementById('editOverview').value = item.overview || '';
            alert('✅ TMDB Match: ' + (item.title || item.name));
          } else {
            alert('Walang nahanap sa TMDB.');
          }
        }).catch(function() { alert('TMDB Fetch Error'); });
    }

    function saveMovie() {
      var payload = {
        id: document.getElementById('editId').value,
        file_name: document.getElementById('editTitle').value,
        poster: document.getElementById('editPoster').value,
        year: document.getElementById('editYear').value,
        rating: document.getElementById('editRating').value,
        category: document.getElementById('editCategory').value,
        overview: document.getElementById('editOverview').value
      };

      fetch('/api/admin/update-movie', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      })
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.success) {
          closeModal();
          loadMovies();
        } else {
          alert('Error: ' + d.error);
        }
      });
    }

    function deleteItem(encId) {
      var id = decodeURIComponent(encId);
      if (!confirm('Burahin ang pelikulang ito?')) return;

      fetch('/api/admin/delete-movie/' + encodeURIComponent(id), { method: 'DELETE' })
        .then(function(r) { return r.json(); })
        .then(function(d) {
          if (d.success) {
            loadMovies();
          } else {
            alert('Failed to delete');
          }
        });
    }
  </script>
</body>
</html>`);
});
// 10. SERVER START
// ---------------------------------------------------------------------------
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 PINOMAX Server live on port ${PORT}`);
});