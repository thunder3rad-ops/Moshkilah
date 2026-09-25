import express from 'express';
import type { Request, Response } from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const isProd = process.env.NODE_ENV === 'production';
const PORT = process.env.PORT || 3000;

const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'submissions.json');

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Initial seed data if file does not exist
const INITIAL_SEED_SUBMISSIONS = [
  {
    id: 'sub_101',
    trackingCode: '8492',
    type: 'فكرة',
    title: 'تطبيق نظام العمل المرن وتوسيع ساعات الحضور',
    content: 'أقترح إتاحة مرونة في ساعات الحضور الصباحية بحيث يمكن للموظف البدء بين 7:30 صباحاً إلى 9:00 صباحاً وإكمال ساعات العمل المعتمدة. هذا سيقلل من ضغط زحام السير ويزيد من الإنتاجية والراحة النفسية للفريق.',
    createdAt: '2026-09-24T05:15:00.000Z',
    votes: 14,
    status: 'قيد الدراسة',
    adminResponse: {
      author: 'إدارة الموارد البشرية',
      text: 'فكرة ممتازة ومحل اهتمام كبير. يجري حالياً إعداد دراسة أثر لتجربة هذا النموذج لمدة شهر تجريبي في الربع القادم.',
      respondedAt: '2026-09-24T06:00:00.000Z',
    },
  },
  {
    id: 'sub_102',
    trackingCode: '6134',
    type: 'شكوى',
    title: 'ضعف التكييف في الطابق الثاني بقاعة الاجتماعات الرئيسية',
    content: 'نواجه مشكلة متكررة منذ أسبوعين في نظام التكييف المركزي بقاعة الاجتماعات رقم 2B، حيث تصبح القاعة شديدة الحرارة أثناء ورش العمل والاجتماعات الطويلة مما يعيق التركيز.',
    createdAt: '2026-09-23T14:40:00.000Z',
    votes: 8,
    status: 'تمت المعالجة',
    adminResponse: {
      author: 'فريق التشغيل والصيانة',
      text: 'تم استدعاء فريق الصيانة المتخصص وتغيير مضخة التبريد وإعادة ضبط درجات الحرارة. القاعة الآن جاهزة وتعمل بكفاءة تامة.',
      respondedAt: '2026-09-23T17:20:00.000Z',
    },
  },
  {
    id: 'sub_103',
    trackingCode: '3920',
    type: 'فكرة',
    title: 'تخصيص ركن هادئ للقراءة والاستراحة المعرفية',
    content: 'نقترح تحويل المساحة غير المستغلة بجانب شرفة الطابق الثالث إلى ركن هادئ يحتوي على مقاعد مريحة ومكتبة مصغرة للكتب التخصصية والمهنية لتجديد طاقة الموظفين خلال فترات الاستراحة.',
    createdAt: '2026-09-24T04:20:00.000Z',
    votes: 21,
    status: 'تم الاطلاع',
    adminResponse: null,
  },
];

function readSubmissions(): any[] {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const content = fs.readFileSync(DATA_FILE, 'utf-8');
      const parsed = JSON.parse(content);
      if (Array.isArray(parsed)) {
        return parsed;
      }
    }
  } catch (err) {
    console.error('Error reading submissions file:', err);
  }

  // If file doesn't exist or is empty, write initial seed
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(INITIAL_SEED_SUBMISSIONS, null, 2), 'utf-8');
  } catch (e) {
    console.error('Failed writing initial seed:', e);
  }
  return INITIAL_SEED_SUBMISSIONS;
}

function writeSubmissions(submissions: any[]) {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(submissions, null, 2), 'utf-8');
  } catch (err) {
    console.error('Error writing submissions file:', err);
  }
}

// SSE Connected Clients Set
const sseClients = new Set<Response>();

function broadcast(eventType: string, payload: any) {
  const message = `event: ${eventType}\ndata: ${JSON.stringify(payload)}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(message);
    } catch {
      sseClients.delete(client);
    }
  }
}

// Periodic keep-alive ping to prevent client connection timeout
setInterval(() => {
  for (const client of sseClients) {
    try {
      client.write(': keep-alive\n\n');
    } catch {
      sseClients.delete(client);
    }
  }
}, 15000);

async function startServer() {
  const app = express();
  app.use(express.json());

  // SSE Live Events Stream
  app.get('/api/events', (req: Request, res: Response) => {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.flushHeaders?.();

    // Send initial greeting
    res.write(`event: init\ndata: ${JSON.stringify({ connected: true, time: Date.now() })}\n\n`);

    sseClients.add(res);

    req.on('close', () => {
      sseClients.delete(res);
    });
    res.on('error', () => {
      sseClients.delete(res);
    });
  });

  // API Config
  app.get('/api/config', (_req: Request, res: Response) => {
    let appUrl = process.env.APP_URL || '';
    let publicUrl = appUrl;
    if (publicUrl.includes('ais-dev-')) {
      publicUrl = publicUrl.replace('ais-dev-', 'ais-pre-');
    }
    if (!publicUrl) {
      publicUrl = 'https://ais-pre-abqdc7ru3raaj3pjciki3u-624162038251.europe-west2.run.app';
    }
    res.json({
      publicUrl,
      appUrl,
    });
  });

  // Get all submissions
  app.get('/api/submissions', (_req: Request, res: Response) => {
    const list = readSubmissions();
    res.json(list);
  });

  // Create new submission
  app.post('/api/submissions', (req: Request, res: Response) => {
    const { type, title, content } = req.body;
    if (!type || !content) {
      return res.status(400).json({ error: 'النوع والمحتوى مطلوبان' });
    }

    const current = readSubmissions();
    const newSubmission = {
      id: 'sub_' + Date.now() + '_' + Math.random().toString(36).substring(2, 7),
      trackingCode: Math.floor(1000 + Math.random() * 9000).toString(),
      type: type || 'فكرة',
      title: title?.trim() || '',
      content: content.trim(),
      createdAt: new Date().toISOString(),
      votes: 1,
      status: 'جديد',
      adminResponse: null,
    };

    current.unshift(newSubmission);
    writeSubmissions(current);

    // Real-time broadcast to all connected clients
    broadcast('new_submission', newSubmission);
    broadcast('submissions_sync', current);

    res.status(201).json(newSubmission);
  });

  // Vote for a submission
  app.post('/api/submissions/:id/vote', (req: Request, res: Response) => {
    const { id } = req.params;
    const current = readSubmissions();
    const item = current.find((s) => s.id === id);
    if (!item) {
      return res.status(404).json({ error: 'المشاركة غير موجودة' });
    }

    item.votes = (item.votes || 0) + 1;
    writeSubmissions(current);

    // Broadcast vote update
    broadcast('submission_voted', item);

    res.json(item);
  });

  // Update submission status (HR Admin)
  app.post('/api/submissions/:id/status', (req: Request, res: Response) => {
    const { id } = req.params;
    const { status } = req.body;
    const current = readSubmissions();
    const item = current.find((s) => s.id === id);
    if (!item) {
      return res.status(404).json({ error: 'المشاركة غير موجودة' });
    }

    item.status = status;
    writeSubmissions(current);

    // Broadcast status change
    broadcast('status_changed', item);

    res.json(item);
  });

  // Add official admin response
  app.post('/api/submissions/:id/response', (req: Request, res: Response) => {
    const { id } = req.params;
    const { author, text } = req.body;
    const current = readSubmissions();
    const item = current.find((s) => s.id === id);
    if (!item) {
      return res.status(404).json({ error: 'المشاركة غير موجودة' });
    }

    item.adminResponse = {
      author: author || 'إدارة صوت الموظفين',
      text: text?.trim(),
      respondedAt: new Date().toISOString(),
    };
    writeSubmissions(current);

    // Broadcast admin response
    broadcast('admin_response', item);

    res.json(item);
  });

  // Delete submission
  app.delete('/api/submissions/:id', (req: Request, res: Response) => {
    const { id } = req.params;
    let current = readSubmissions();
    current = current.filter((s) => s.id !== id);
    writeSubmissions(current);

    // Broadcast delete
    broadcast('submission_deleted', { id });

    res.json({ success: true });
  });

  // Vite Integration (Dev vs Prod)
  if (!isProd) {
    const { createServer } = await import('vite');
    const vite = await createServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(__dirname, 'dist');
    app.use(express.static(distPath));
    app.get('*', (_req: Request, res: Response) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  app.listen(PORT, () => {
    console.log(`Server listening on http://localhost:${PORT}`);
  });
}

startServer().catch((err) => {
  console.error('Failed to start server:', err);
});
