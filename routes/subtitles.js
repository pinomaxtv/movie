import express from 'express';
import axios from 'axios';
import AdmZip from 'adm-zip';

const router = express.Router();
const TMDB_API_KEY = process.env.TMDB_API_KEY || '86fd55697899e8444fa3da3ddd24518d';
const RAILWAY_URL = (process.env.APP_URL || 'https://pinomax-streambot.up.railway.app').replace(/\/+$/, '');

// 🟢 SUBTITLE SCRAPER & IN-MEMORY UNZIPPER (SUBDL)
router.get('/api/subtitles/:query', async (req, res) => {
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
    // Isinama ang 'fil' para sa mga nakatag bilang Filipino/Pilipino
    const subdlUrl = `https://api.subdl.com/api/v1/subtitles?api_key=${subdlKey}&tmdb_id=${tmdbId}&languages=tl,fil,en`;

    const subRes = await axios.get(subdlUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K)' },
      timeout: 8000
    });

    const subtitlesData = subRes.data?.subtitles || [];
    const formatted = subtitlesData.map(sub => {
      const langCode = (sub.lang || sub.language || '').toLowerCase();
      const isTag = langCode.includes('tag') || langCode.includes('tl') || langCode.includes('fil') || langCode.includes('pil');
      const rawUrl = sub.url.startsWith('/') ? `https://dl.subdl.com${sub.url}` : sub.url;
      const release = sub.release_name ? sub.release_name.substring(0, 16) : 'HD';

      return {
        name: isTag ? `🇵🇭 Tagalog / Filipino (${release})` : `🇬🇧 English (${release})`,
        // Ginawa nang Absolute URL patungong Railway para hindi maligaw ang Cloudflare Worker
        url: `${RAILWAY_URL}/api/sub-content?url=${encodeURIComponent(rawUrl)}`,
        type: 'vtt'
      };
    });

    res.json({ success: formatted.length > 0, subtitles: formatted, tmdbId });
  } catch (err) {
    res.json({ success: false, subtitles: [] });
  }
});

router.get('/api/sub-content', async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).send('URL required');

  try {
    const response = await axios.get(targetUrl, {
      responseType: 'arraybuffer',
      headers: { 'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K)' },
      timeout: 10000
    });

    let srtText = '';
    const buffer = Buffer.from(response.data);

    if (targetUrl.toLowerCase().includes('.zip') || (buffer[0] === 0x50 && buffer[1] === 0x4B)) {
      const zip = new AdmZip(buffer);
      const zipEntries = zip.getEntries();
      const srtEntry = zipEntries.find(e => e.entryName.toLowerCase().endsWith('.srt') || e.entryName.toLowerCase().endsWith('.vtt'));

      if (srtEntry) {
        srtText = srtEntry.getData().toString('utf8');
      } else {
        return res.status(404).send('No .srt found inside zip');
      }
    } else {
      srtText = buffer.toString('utf8');
    }

    if (!srtText.trim().startsWith('WEBVTT')) {
      srtText = 'WEBVTT\n\n' + srtText.replace(/(\d{2}:\d{2}:\d{2}),(\d{3})/g, '$1.$2');
    }

    res.writeHead(200, {
      'Content-Type': 'text/vtt; charset=utf-8',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=86400'
    });
    res.end(srtText);
  } catch (err) {
    res.status(500).send('Failed to process subtitle');
  }
});

export default router;
