# 🎬 PinomaxStreamv1 - High-Speed Video Streaming & MTProto Media Engine

A production-ready, lightweight Personal Video Streaming Portal, Cloudflare-Cached Embed Engine, and Telegram MTProto 2GB Streaming Gateway built with **Node.js (Express.js)**, **GramJS**, **MongoDB Atlas**, **Firebase Realtime Database**, and **Artplayer.js**.

---

## ⚡ Core Features & Capabilities

- 🚀 **Telegram MTProto 2GB Stream Proxy:** Bypasses standard Telegram 20MB Bot API limits using GramJS MTProto client. Streams large videos up to 2GB via **HTTP 206 Partial Content (512KB chunking)**.
- 📱 **Standalone Embed Engine (`/embed/...`):** 100vw by 100vh edge-to-edge video player with zero margins, custom Crimson Red (`#E50914`) theme, built-in download button, and HLS support (ideal for Android WebViews).
- 🛡️ **Cloudflare Caching Worker Ready:** Fully compatible with edge caching to absorb video streaming traffic and eliminate origin server bandwidth consumption.
- 🔥 **Firebase RTDB Synchronization:** Reads and streams directly from `jetmax-f3e8e...` schema (`admin_tagalog_movies`, `admin_community_tagalog_series`, `vivamax`, `anime`).
- 🛠️ **Admin Control Panel (`/admin`):** Live TMDB search modal with 1-click auto-fill and direct Firebase database write operations.

---

## 🌐 Dynamic URL Routes

| Endpoint | Description |
| :--- | :--- |
| `/` | Bento Grid Home Dashboard with category rows |
| `/embed/:fileId` | Clean Standalone Artplayer for Telegram Streams |
| `/embed/movie/:node/:id` | Clean Standalone Embed for Firebase Database items |
| `/stream/:fileId` | High-speed HTTP 206 Partial Range Stream Proxy |
| `/download/:fileId` | Direct Attachment Downloader (Android `DownloadManager` compliant) |
| `/player/:fileId` | Portal Web Player with video telemetry details |
| `/admin` | Content Management Dashboard |

---

## 🔑 Environment Variables Configuration

Set these variables in your **Render.com** Dashboard:

| Variable Name | Description |
| :--- | :--- |
| `BOT_TOKEN` | Telegram Bot Token from `@BotFather` |
| `API_ID` | Telegram App API ID from `my.telegram.org` |
| `API_HASH` | Telegram App API Hash from `my.telegram.org` |
| `ADMIN_ID` | Your Telegram User ID from `@userinfobot` |
| `MONGODB_URI` | MongoDB Atlas Connection String (`mongodb+srv://...`) |
| `FIREBASE_DATABASE_URL` | `https://jetmax-f3e8e-default-rtdb.firebaseio.com` |
| `TMDB_API_KEY` | Free API Key from `themoviedb.org` |
| `APP_URL` | `https://pinomaxstreamv1.onrender.com` |
| `NODE_ENV` | `production` |

---

## 📱 Android App Integration (`DetailsActivity.java`)

To load the stream directly inside your Android App WebView:
```java
String embedUrl = "https://pinomax-cache.roderickalmaras05.workers.dev/embed/" + fileId;
playerWebView.setVisibility(View.VISIBLE);
playerWebView.loadUrl(embedUrl);
