import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { createServer } from 'http';
import { Server } from 'socket.io';
import crypto from 'crypto';
import fs from 'fs';
import nodemailer from 'nodemailer';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer);

const PORT = 3000;
const CLOUD_RUN_PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : null;

// Initialize Firestore Configuration & REST Client
let firebaseAppConfig = null;
let firestoreDbId = '(default)';

function initFirebaseConfig() {
  try {
    const configPath = path.join(__dirname, 'firebase-applet-config.json');
    if (fs.existsSync(configPath)) {
      firebaseAppConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
      firestoreDbId = firebaseAppConfig.firestoreDatabaseId || '(default)';
      console.log(`Firestore client configured with project ${firebaseAppConfig.projectId}, db: ${firestoreDbId}`);
    }
  } catch (err) {
    console.log('Firebase config notice:', err.message);
  }
}
initFirebaseConfig();

function toFirestoreValue(val) {
  if (val === null || val === undefined) return { nullValue: null };
  if (typeof val === 'boolean') return { booleanValue: val };
  if (typeof val === 'number') {
    if (Number.isInteger(val)) return { integerValue: String(val) };
    return { doubleValue: val };
  }
  if (typeof val === 'string') return { stringValue: val };
  if (Array.isArray(val)) {
    return { arrayValue: { values: val.map(toFirestoreValue) } };
  }
  if (typeof val === 'object') {
    const fields = {};
    for (const [k, v] of Object.entries(val)) {
      if (v !== undefined) fields[k] = toFirestoreValue(v);
    }
    return { mapValue: { fields } };
  }
  return { stringValue: String(val) };
}

function fromFirestoreValue(val) {
  if (!val) return null;
  if ('nullValue' in val) return null;
  if ('booleanValue' in val) return val.booleanValue;
  if ('integerValue' in val) return parseInt(val.integerValue, 10);
  if ('doubleValue' in val) return val.doubleValue;
  if ('stringValue' in val) return val.stringValue;
  if ('timestampValue' in val) return val.timestampValue;
  if ('arrayValue' in val) {
    return (val.arrayValue.values || []).map(fromFirestoreValue);
  }
  if ('mapValue' in val) {
    const res = {};
    for (const [k, v] of Object.entries(val.mapValue.fields || {})) {
      res[k] = fromFirestoreValue(v);
    }
    return res;
  }
  return null;
}

const firestoreClient = {
  collection(name) {
    return {
      doc(id) {
        return {
          async set(data) {
            if (!firebaseAppConfig || !firebaseAppConfig.projectId) return;
            try {
              const url = `https://firestore.googleapis.com/v1/projects/${firebaseAppConfig.projectId}/databases/${firestoreDbId}/documents/${name}/${encodeURIComponent(id)}?key=${firebaseAppConfig.apiKey}`;
              const body = { fields: toFirestoreValue(data).mapValue?.fields || {} };
              await fetch(url, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
              });
            } catch (e) {
              // Silently handle offline/network deviations
            }
          },
          async delete() {
            if (!firebaseAppConfig || !firebaseAppConfig.projectId) return;
            try {
              const url = `https://firestore.googleapis.com/v1/projects/${firebaseAppConfig.projectId}/databases/${firestoreDbId}/documents/${name}/${encodeURIComponent(id)}?key=${firebaseAppConfig.apiKey}`;
              await fetch(url, { method: 'DELETE' });
            } catch (e) {
              // Silently handle offline/network deviations
            }
          }
        };
      },
      async get() {
        if (!firebaseAppConfig || !firebaseAppConfig.projectId) return { forEach: () => {}, size: 0 };
        try {
          const url = `https://firestore.googleapis.com/v1/projects/${firebaseAppConfig.projectId}/databases/${firestoreDbId}/documents/${name}?key=${firebaseAppConfig.apiKey}&pageSize=300`;
          const resp = await fetch(url);
          if (!resp.ok) {
            return { forEach: () => {}, size: 0 };
          }
          const json = await resp.json();
          const docs = json.documents || [];
          const items = docs.map(d => {
            const docId = decodeURIComponent(d.name.split('/').pop());
            const docData = fromFirestoreValue({ mapValue: { fields: d.fields } }) || {};
            return { id: docId, data: () => docData };
          });
          return {
            size: items.length,
            forEach(cb) {
              items.forEach(cb);
            }
          };
        } catch (e) {
          return { forEach: () => {}, size: 0 };
        }
      }
    };
  }
};

async function safeFirestoreOperation(operationFn, opName = 'operation') {
  if (!firebaseAppConfig) return;
  try {
    await operationFn(firestoreClient);
  } catch (err) {
    // Graceful fallback to local JSON database
  }
}

// Local cache database helper functions
const DB_FILE = path.join(__dirname, 'db_users.json');

function loadUsersFromFile() {
  try {
    if (!fs.existsSync(DB_FILE)) {
      return {};
    }
    const content = fs.readFileSync(DB_FILE, 'utf8');
    return JSON.parse(content || '{}');
  } catch (error) {
    console.error('Error loading users from file:', error);
    return {};
  }
}

function saveUsersToFile(users) {
  try {
    fs.writeFileSync(DB_FILE, JSON.stringify(users, null, 2), 'utf8');
  } catch (error) {
    console.error('Error saving users to file:', error);
  }
}

const inMemoryUsers = loadUsersFromFile();

function loadUsers() {
  return inMemoryUsers;
}

function saveUser(userId, userData) {
  inMemoryUsers[userId] = userData;
  saveUsersToFile(inMemoryUsers);
  safeFirestoreOperation(async (db) => {
    await db.collection('users').doc(userId).set(userData, { merge: true });
  }, `saveUser(${userId})`);
}

function deleteUser(userId) {
  delete inMemoryUsers[userId];
  saveUsersToFile(inMemoryUsers);
  safeFirestoreOperation(async (db) => {
    await db.collection('users').doc(userId).delete();
  }, `deleteUser(${userId})`);
}

const pendingRegistrations = {};

function hasBadContent(text) {
  if (!text) return false;
  const originalLower = String(text).toLowerCase();
  
  if (originalLower.includes('1488')) {
    return true;
  }

  // Remove all non-alphanumeric characters (spaces, underscores, dots, hyphens, etc.)
  const clean = originalLower.replace(/[^a-z0-9а-яё]/gi, '');

  if (clean.includes('1488')) {
    return true;
  }

  // Map numbers to letters for simple leetspeak checks
  const normalized = clean.replace(/[0-9]/g, (m) => {
    if (m === '0') return 'o';
    if (m === '1') return 'i';
    if (m === '3') return 'e';
    if (m === '4') return 'a';
    if (m === '5') return 's';
    if (m === '7') return 't';
    if (m === '8') return 'b';
    return m;
  });

  const badPatterns = [
    // Russian swear roots
    'хуй', 'хуя', 'хуи', 'хуе', 'пизд', 'залуп', 'пидор', 'пидар', 'педер', 'еба', 'ебт', 'ебл', 'бля', 'сук', 'муд', 'ганд', 'гонд', 'шлюх', 'член', 'говн', 'гавн', 'ублю', 'дроч',
    // Transliterated Russian
    'hui', 'huy', 'pizd', 'zalup', 'pidor', 'pidar', 'peder', 'ebal', 'ebat', 'ebla', 'blya', 'suka', 'mudak', 'gandon', 'shluh', 'chlen', 'govno', 'gavno', 'ublyu', 'droch',
    // English swear roots
    'fuck', 'shit', 'bitch', 'cunt', 'dick', 'pussy', 'assh', 'bastard', 'whore'
  ];

  for (const pat of badPatterns) {
    if (normalized.includes(pat) || originalLower.includes(pat) || clean.includes(pat)) {
      return true;
    }
  }
  return false;
}

const SESSIONS_FILE = path.join(__dirname, 'db_sessions.json');

function loadSessionsFromFile() {
  try {
    if (!fs.existsSync(SESSIONS_FILE)) {
      return {};
    }
    const content = fs.readFileSync(SESSIONS_FILE, 'utf8');
    return JSON.parse(content || '{}');
  } catch (error) {
    console.error('Error loading sessions from file:', error);
    return {};
  }
}

function saveSessionsToFile(sessions) {
  try {
    fs.writeFileSync(SESSIONS_FILE, JSON.stringify(sessions, null, 2), 'utf8');
  } catch (error) {
    console.error('Error saving sessions to file:', error);
  }
}

const activeSessions = loadSessionsFromFile();

function saveSession(token, sessionData) {
  activeSessions[token] = sessionData;
  saveSessionsToFile(activeSessions);
  safeFirestoreOperation(async (db) => {
    await db.collection('sessions').doc(token).set(sessionData);
  }, `saveSession(${token.substring(0, 8)})`);
}

function deleteSession(token) {
  delete activeSessions[token];
  saveSessionsToFile(activeSessions);
  safeFirestoreOperation(async (db) => {
    await db.collection('sessions').doc(token).delete();
  }, `deleteSession(${token.substring(0, 8)})`);
}

// Sync startup data with Firestore cloud DB
async function syncWithFirestoreOnStart() {
  safeFirestoreOperation(async (db) => {
    console.log('Fetching existing users from Firestore cloud...');
    const usersSnapshot = await db.collection('users').get();
    usersSnapshot.forEach(doc => {
      inMemoryUsers[doc.id] = doc.data();
    });
    saveUsersToFile(inMemoryUsers);
    console.log(`Loaded ${usersSnapshot.size} user account(s) from Firestore cloud database.`);

    const sessionsSnapshot = await db.collection('sessions').get();
    sessionsSnapshot.forEach(doc => {
      activeSessions[doc.id] = doc.data();
    });
    saveSessionsToFile(activeSessions);
    console.log(`Loaded ${sessionsSnapshot.size} active session(s) from Firestore cloud database.`);
  }, 'syncWithFirestoreOnStart');
}
syncWithFirestoreOnStart();

// Helper: ConnectedSockets for accurate online tracking
const connectedUserSockets = new Map();

function isUserOnline(username) {
  if (!username) return false;
  const targetLower = String(username).toLowerCase();
  for (const [sId, uLower] of connectedUserSockets.entries()) {
    if (uLower === targetLower) return true;
  }
  return false;
}

// Helper: Hashing
function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.pbkdf2Sync(password, salt, 1000, 64, 'sha512').toString('hex');
  return `${salt}:${hash}`;
}

function verifyPassword(password, stored) {
  const [salt, hash] = stored.split(':');
  const verifyHash = crypto.pbkdf2Sync(password, salt, 1000, 64, 'sha512').toString('hex');
  return hash === verifyHash;
}

// Helper: Mail Sender
async function sendVerificationEmail(email, code) {
  const host = process.env.SMTP_HOST;
  const port = parseInt(process.env.SMTP_PORT || '587', 10);
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;

  if (!host || !user || !pass) {
    console.log(`\n============================================\n[SMTP fallback] Verification code for ${email} is: ${code}\n============================================\n`);
    return false;
  }

  try {
    const transporter = nodemailer.createTransport({
      host,
      port,
      secure: port === 465,
      auth: { user, pass }
    });

    await transporter.sendMail({
      from: `"SoccerPool Accounts" <${user}>`,
      to: email,
      subject: 'Код подтверждения SoccerPool',
      text: `Ваш 6-значный код подтверждения: ${code}\nДействителен 10 минут.`,
      html: `
        <div style="font-family: sans-serif; max-width: 500px; margin: 0 auto; padding: 20px; border: 1px solid #333; background: #1a1a1a; color: #fff; border-radius: 12px;">
          <h2 style="color: #2ea043; text-align: center; margin-top: 0;">SoccerPool Accounts</h2>
          <p>Вы регистрируете аккаунт в SoccerPool.</p>
          <p>Пожалуйста, введите следующий 6-значный код подтверждения:</p>
          <div style="background: #2a2a2a; padding: 15px; border-radius: 8px; font-size: 26px; font-weight: bold; text-align: center; letter-spacing: 6px; margin: 20px 0; color: #2ea043; border: 1px solid #444;">
            ${code}
          </div>
          <p style="font-size: 12px; color: #888; text-align: center; margin-bottom: 0;">Код действителен в течение 10 минут.</p>
        </div>
      `
    });
    return true;
  } catch (err) {
    console.error('Failed to send SMTP email:', err);
    return false;
  }
}

// Health check endpoints for Cloud Run, Kubernetes, and load balancers
app.get(['/api/health', '/health', '/_ah/health'], (req, res) => {
  res.status(200).json({ status: 'ok', timestamp: Date.now() });
});

// Express middlewares for JSON parsing (limit 20MB for custom skin dataURLs)
app.use(express.json({ limit: '20mb' }));
app.use(express.urlencoded({ extended: true, limit: '20mb' }));

function generateDefaultCampaignOrder() {
  const order = [];
  for (let ch = 0; ch < 5; ch++) {
    const baseStart = ch * 5;
    const firstFour = [baseStart, baseStart + 1, baseStart + 2, baseStart + 3];
    for (let i = firstFour.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      const temp = firstFour[i];
      firstFour[i] = firstFour[j];
      firstFour[j] = temp;
    }
    order.push(...firstFour, baseStart + 4);
  }
  return order;
}

// Auth and sync routes
app.post('/api/auth/register', async (req, res) => {
  const { username, password, confirmPassword, captchaAnswer, captchaNum1, captchaNum2 } = req.body;

  if (!username || !password || !confirmPassword) {
    return res.status(400).json({ success: false, message: 'Заполните все обязательные поля!' });
  }

  // Math Captcha validation
  const num1 = parseInt(captchaNum1, 10);
  const num2 = parseInt(captchaNum2, 10);
  const ans = parseInt(captchaAnswer, 10);
  if (isNaN(num1) || isNaN(num2) || isNaN(ans) || (num1 + num2 !== ans)) {
    return res.status(400).json({ success: false, message: 'Неверное решение капчи! Пройдите проверку заново.' });
  }

  const usernameRegex = /^[a-zA-Z0-9_]{3,20}$/;
  if (!usernameRegex.test(username)) {
    return res.status(400).json({ success: false, message: 'Логин должен быть от 3 до 20 символов и содержать только латинские буквы, цифры и _!' });
  }

  if (hasBadContent(username)) {
    return res.status(400).json({ success: false, message: 'Логин содержит недопустимые или оскорбительные выражения!' });
  }

  if (password.length < 6) {
    return res.status(400).json({ success: false, message: 'Пароль должен быть не менее 6 символов!' });
  }

  if (password !== confirmPassword) {
    return res.status(400).json({ success: false, message: 'Пароли не совпадают!' });
  }

  const users = loadUsers();
  const usernameLower = username.toLowerCase();

  for (const uid in users) {
    if (users[uid].username.toLowerCase() === usernameLower) {
      return res.status(400).json({ success: false, message: 'Этот логин уже занят!' });
    }
  }

  const userId = usernameLower;
  const defaultCampOrder = generateDefaultCampaignOrder();
  const newUser = {
    username: username,
    email: "",
    password: hashPassword(password),
    createdAt: new Date().toISOString(),
    displayName: username,
    avatar: "⚽",
    data: {
      camp_order: defaultCampOrder
    }
  };

  saveUser(userId, newUser);

  const token = crypto.randomBytes(32).toString('hex');
  saveSession(token, { userId, username: username, email: "" });

  res.json({
    success: true,
    message: 'Аккаунт успешно создан!',
    token,
    username: username,
    email: "",
    displayName: username,
    avatar: "⚽",
    data: newUser.data
  });
});

app.post('/api/auth/send-code', async (req, res) => {
  const { email, username, password, confirmPassword } = req.body;

  if (!email || !username || !password || !confirmPassword) {
    return res.status(400).json({ success: false, message: 'Все поля обязательны к заполнению!' });
  }

  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    return res.status(400).json({ success: false, message: 'Некорректный формат почты!' });
  }

  const usernameRegex = /^[a-zA-Z0-9_]{3,20}$/;
  if (!usernameRegex.test(username)) {
    return res.status(400).json({ success: false, message: 'Логин должен быть от 3 до 20 символов и содержать только латинские буквы, цифры и символ подчеркивания!' });
  }

  if (hasBadContent(username)) {
    return res.status(400).json({ success: false, message: 'Логин содержит недопустимые или оскорбительные выражения!' });
  }

  if (password.length < 6) {
    return res.status(400).json({ success: false, message: 'Пароль должен быть не менее 6 символов!' });
  }

  if (password !== confirmPassword) {
    return res.status(400).json({ success: false, message: 'Пароли не совпадают!' });
  }

  const users = loadUsers();
  const emailLower = email.toLowerCase();
  const usernameLower = username.toLowerCase();
  
  for (const uid in users) {
    if (users[uid].email && users[uid].email.toLowerCase() === emailLower) {
      return res.status(400).json({ success: false, message: 'Пользователь с такой почтой уже зарегистрирован!' });
    }
    if (users[uid].username.toLowerCase() === usernameLower) {
      return res.status(400).json({ success: false, message: 'Этот логин уже занят!' });
    }
  }

  const code = String(100000 + Math.floor(Math.random() * 900000));
  
  pendingRegistrations[emailLower] = {
    username,
    email,
    passwordHash: hashPassword(password),
    code,
    expiresAt: Date.now() + 10 * 60 * 1000
  };

  const sent = await sendVerificationEmail(email, code);

  res.json({
    success: true,
    message: 'Код подтверждения отправлен на вашу почту!',
    devFallback: !sent ? code : null
  });
});

app.post('/api/auth/register-confirm', async (req, res) => {
  const { email, code } = req.body;

  if (!email || !code) {
    return res.status(400).json({ success: false, message: 'Почта и код подтверждения обязательны!' });
  }

  const emailLower = email.toLowerCase();
  const pending = pendingRegistrations[emailLower];

  if (!pending) {
    return res.status(400).json({ success: false, message: 'Регистрационная сессия не найдена. Попробуйте заново.' });
  }

  if (Date.now() > pending.expiresAt) {
    delete pendingRegistrations[emailLower];
    return res.status(400).json({ success: false, message: 'Срок действия кода истек! Запросите код заново.' });
  }

  if (pending.code !== String(code).trim()) {
    return res.status(400).json({ success: false, message: 'Неверный код подтверждения!' });
  }

  const users = loadUsers();
  const userId = pending.username.toLowerCase();
  const defaultCampOrder = generateDefaultCampaignOrder();

  saveUser(userId, {
    username: pending.username,
    email: pending.email,
    password: pending.passwordHash,
    createdAt: new Date().toISOString(),
    displayName: pending.username,
    avatar: "⚽",
    data: {
      camp_order: defaultCampOrder
    }
  });
  delete pendingRegistrations[emailLower];

  const token = crypto.randomBytes(32).toString('hex');
  saveSession(token, { userId, username: pending.username, email: pending.email });

  res.json({
    success: true,
    message: 'Аккаунт успешно создан!',
    token,
    username: pending.username,
    email: pending.email,
    displayName: pending.username,
    avatar: "⚽",
    data: {
      camp_order: defaultCampOrder
    }
  });
});

app.post('/api/auth/login', async (req, res) => {
  const { usernameOrEmail, password } = req.body;

  if (!usernameOrEmail || !password) {
    return res.status(400).json({ success: false, message: 'Заполните все поля!' });
  }

  const users = loadUsers();
  const targetLower = usernameOrEmail.toLowerCase();
  let foundUser = null;
  let userId = null;

  for (const uid in users) {
    if (users[uid].username.toLowerCase() === targetLower || (users[uid].email && users[uid].email.toLowerCase() === targetLower)) {
      foundUser = users[uid];
      userId = uid;
      break;
    }
  }

  if (!foundUser || !verifyPassword(password, foundUser.password)) {
    return res.status(400).json({ success: false, message: 'Неверный логин/почта или пароль!' });
  }

  if (foundUser.deactivated) {
    return res.status(403).json({ success: false, message: 'Ваш аккаунт деактивирован администратором!' });
  }

  if (!foundUser.data) foundUser.data = {};
  if (!foundUser.data.camp_order || !Array.isArray(foundUser.data.camp_order) || foundUser.data.camp_order.length !== 25) {
    foundUser.data.camp_order = generateDefaultCampaignOrder();
    saveUser(userId, foundUser);
  }

  const token = crypto.randomBytes(32).toString('hex');
  saveSession(token, { userId, username: foundUser.username, email: foundUser.email || "" });

  res.json({
    success: true,
    message: 'Вход успешно выполнен!',
    token,
    username: foundUser.username,
    email: foundUser.email || "",
    displayName: foundUser.displayName || foundUser.username,
    avatar: foundUser.avatar || "⚽",
    isDev: !!foundUser.isDev,
    data: foundUser.data || {}
  });
});

app.post('/api/user/update-email', (req, res) => {
  const authHeader = req.headers.authorization;
  let token = null;

  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7);
  }

  if (!token || !activeSessions[token]) {
    return res.status(401).json({ success: false, message: 'Неавторизован!' });
  }

  const session = activeSessions[token];
  const users = loadUsers();
  const user = users[session.userId];

  if (!user) {
    return res.status(404).json({ success: false, message: 'Пользователь не найден!' });
  }

  const { email } = req.body;
  if (!email) {
    return res.status(400).json({ success: false, message: 'Введите адрес почты!' });
  }

  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    return res.status(400).json({ success: false, message: 'Некорректный формат почты!' });
  }

  const emailLower = email.toLowerCase();
  for (const uid in users) {
    if (uid !== session.userId && users[uid].email && users[uid].email.toLowerCase() === emailLower) {
      return res.status(400).json({ success: false, message: 'Эта почта уже привязана к другому аккаунту!' });
    }
  }

  user.email = email;
  saveUser(session.userId, user);
  session.email = email;
  saveSession(token, session);

  res.json({ success: true, message: 'Почта успешно привязана к аккаунту!', email });
});

app.post('/api/user/sync', (req, res) => {
  const authHeader = req.headers.authorization;
  let token = null;

  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7);
  } else if (req.body.token) {
    token = req.body.token;
  }

  if (!token || !activeSessions[token]) {
    return res.status(401).json({ success: false, message: 'Неавторизован!' });
  }

  const session = activeSessions[token];
  const users = loadUsers();
  const user = users[session.userId];

  if (!user) {
    return res.status(404).json({ success: false, message: 'Пользователь не найден!' });
  }

  user.data = req.body.data || {};
  if (req.body.data && req.body.data.user_mmr !== undefined) {
    user.user_mmr = parseInt(req.body.data.user_mmr, 10) || 0;
  }
  saveUser(session.userId, user);

  res.json({ success: true, isDev: !!user.isDev, message: 'Прогресс успешно синхронизирован с облаком!' });
});

app.post('/api/user/enable-dev', (req, res) => {
  const pin = req.query.pin || (req.body && req.body.pin) || req.headers['x-dev-pin'];
  const authHeader = req.headers.authorization;
  let token = null;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7);
  }

  if (pin !== '2831') {
    return res.status(403).json({ success: false, message: 'Неверный пароль разработчика!' });
  }

  if (token && activeSessions[token]) {
    const session = activeSessions[token];
    const users = loadUsers();
    const user = users[session.userId];
    if (user) {
      user.isDev = true;
      saveUser(session.userId, user);
      return res.json({ success: true, isDev: true, message: 'Режим разработчика привязан к вашему аккаунту!' });
    }
  }

  res.json({ success: true, isDev: true, message: 'Режим разработчика активирован!' });
});

app.post('/api/user/disable-dev', (req, res) => {
  const authHeader = req.headers.authorization;
  let token = null;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7);
  }

  if (token && activeSessions[token]) {
    const session = activeSessions[token];
    const users = loadUsers();
    const user = users[session.userId];
    if (user) {
      user.isDev = false;
      saveUser(session.userId, user);
    }
  }

  res.json({ success: true, isDev: false, message: 'Режим разработчика отключен на аккаунте.' });
});

app.post('/api/user/update-profile', (req, res) => {
  const authHeader = req.headers.authorization;
  let token = null;

  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7);
  }

  if (!token || !activeSessions[token]) {
    return res.status(401).json({ success: false, message: 'Неавторизован!' });
  }

  const session = activeSessions[token];
  const users = loadUsers();
  const user = users[session.userId];

  if (!user) {
    return res.status(404).json({ success: false, message: 'Пользователь не найден!' });
  }

  const { displayName, avatar, selectedTitle } = req.body;
  
  if (displayName !== undefined) {
    const trimmed = String(displayName).trim();
    if (hasBadContent(trimmed)) {
      return res.status(400).json({ success: false, message: 'Никнейм содержит недопустимые или оскорбительные выражения!' });
    }
    if (trimmed.length > 0 && trimmed.length <= 25) {
      user.displayName = trimmed;
    } else if (trimmed.length === 0) {
      user.displayName = user.username;
    } else {
      return res.status(400).json({ success: false, message: 'Никнейм должен быть не более 25 символов!' });
    }
  }

  if (avatar !== undefined) {
    user.avatar = avatar;
  }

  if (selectedTitle !== undefined) {
    user.selectedTitle = String(selectedTitle).trim();
  }

  saveUser(session.userId, user);

  res.json({ 
    success: true, 
    message: 'Профиль успешно обновлен!', 
    displayName: user.displayName || user.username,
    avatar: user.avatar || "⚽",
    selectedTitle: user.selectedTitle || "🌱 Новичок"
  });
});

// Leaderboard and Friends API
app.get('/api/leaderboard', (req, res) => {
  const users = loadUsers();
  const leaderboard = [];

  for (const uid in users) {
    const u = users[uid];
    const mmr = parseInt(u.user_mmr || (u.data && u.data.user_mmr) || 0, 10);
    leaderboard.push({
      username: u.username,
      displayName: u.displayName || u.username,
      avatar: u.avatar || "⚽",
      mmr: mmr,
      selectedTitle: u.selectedTitle || "🌱 Новичок"
    });
  }

  leaderboard.sort((a, b) => b.mmr - a.mmr);

  const topList = leaderboard.slice(0, 50).map((item, index) => ({
    rank: index + 1,
    ...item
  }));

  res.json({ success: true, leaderboard: topList, totalPlayers: leaderboard.length });
});

function isDevAuthorized(req) {
  const pin = req.query.pin || (req.body && req.body.pin) || req.headers['x-dev-pin'];
  if (pin === '2831') return true;

  const authHeader = req.headers.authorization;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    const token = authHeader.substring(7);
    if (activeSessions[token]) {
      const users = loadUsers();
      const user = users[activeSessions[token].userId];
      if (user && user.isDev) return true;
    }
  }
  return false;
}

app.get('/api/user/profile/:username', (req, res) => {
  const users = loadUsers();
  const targetLower = String(req.params.username || '').replace(/^@/, '').trim().toLowerCase();
  
  let targetUser = null;
  for (const uid in users) {
    if (users[uid].username && users[uid].username.toLowerCase() === targetLower) {
      targetUser = users[uid];
      break;
    }
  }

  if (!targetUser) {
    return res.status(404).json({ success: false, message: 'Игрок не найден' });
  }

  // Check requesting user from authorization header
  const authHeader = req.headers.authorization;
  let token = null;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7);
  }

  let requestingUser = null;
  if (token && activeSessions[token]) {
    requestingUser = users[activeSessions[token].userId];
  }

  const isMe = requestingUser && requestingUser.username && requestingUser.username.toLowerCase() === targetLower;
  const isDev = requestingUser && !!requestingUser.isDev;
  let isFriend = false;
  if (requestingUser && requestingUser.friends) {
    isFriend = requestingUser.friends.some(fId => {
      const fUser = users[fId];
      return fUser && fUser.username && fUser.username.toLowerCase() === targetLower;
    });
  }

  const showOnlineStatus = isMe || isDev || isFriend;

  const allUsers = Object.values(users).map(u => ({
    username: u.username,
    mmr: parseInt(u.user_mmr || (u.data && u.data.user_mmr) || 0, 10)
  })).sort((a, b) => b.mmr - a.mmr);

  const rankIdx = allUsers.findIndex(u => u.username.toLowerCase() === targetLower);
  const rank = rankIdx !== -1 ? rankIdx + 1 : '-';

  const isOnline = isUserOnline(targetUser.username);
  const mmr = parseInt(targetUser.user_mmr || (targetUser.data && targetUser.data.user_mmr) || 0, 10);

  res.json({
    success: true,
    user: {
      username: targetUser.username,
      displayName: targetUser.displayName || targetUser.username,
      avatar: targetUser.avatar || "⚽",
      selectedTitle: targetUser.selectedTitle || "🌱 Новичок",
      mmr: mmr,
      rank: rank,
      isOnline: isOnline,
      showOnlineStatus: showOnlineStatus,
      isFriend: isFriend,
      createdAt: targetUser.createdAt || null
    }
  });
});

app.get('/api/admin/users', (req, res) => {
  if (!isDevAuthorized(req)) {
    return res.status(403).json({ success: false, message: 'Неверный пароль разработчика!' });
  }

  const users = loadUsers();
  const userList = [];

  for (const uid in users) {
    const u = users[uid];
    const mmr = parseInt(u.user_mmr || (u.data && u.data.user_mmr) || 0, 10);

    userList.push({
      uid: uid,
      username: u.username,
      displayName: u.displayName || u.username,
      email: u.email || '—',
      avatar: u.avatar || '⚽',
      selectedTitle: u.selectedTitle || '🌱 Новичок',
      createdAt: u.createdAt || 'Неизвестно',
      mmr: mmr,
      isOnline: isUserOnline(u.username),
      deactivated: !!u.deactivated,
      friendsCount: (u.friends || []).length
    });
  }

  userList.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

  res.json({
    success: true,
    totalCount: userList.length,
    users: userList
  });
});

app.post('/api/admin/user/toggle-active', (req, res) => {
  if (!isDevAuthorized(req)) {
    return res.status(403).json({ success: false, message: 'Неверный пароль разработчика!' });
  }

  const { targetUsername } = req.body || {};
  if (!targetUsername) return res.status(400).json({ success: false, message: 'Не указан никнейм пользователя!' });

  const users = loadUsers();
  const targetLower = String(targetUsername).toLowerCase();
  let foundUid = null;
  let targetUser = null;

  for (const uid in users) {
    if (users[uid].username && users[uid].username.toLowerCase() === targetLower) {
      foundUid = uid;
      targetUser = users[uid];
      break;
    }
  }

  if (!targetUser) {
    return res.status(404).json({ success: false, message: 'Пользователь не найден!' });
  }

  targetUser.deactivated = !targetUser.deactivated;
  saveUser(foundUid, targetUser);

  // If deactivated, purge sessions and disconnect active sockets
  if (targetUser.deactivated) {
    for (const token in activeSessions) {
      if (activeSessions[token].userId && activeSessions[token].userId.toLowerCase() === targetLower) {
        deleteSession(token);
      }
    }
    for (const [sId, uLower] of connectedUserSockets.entries()) {
      if (uLower === targetLower) {
        const socket = io.sockets.sockets.get(sId);
        if (socket) {
          socket.emit('accountDeactivated');
          socket.disconnect(true);
        }
      }
    }
  }

  res.json({
    success: true,
    deactivated: targetUser.deactivated,
    message: targetUser.deactivated 
      ? `Пользователь @${targetUser.username} деактивирован! Он выгнан из системы.` 
      : `Пользователь @${targetUser.username} успешно активирован!`
  });
});

app.post('/api/admin/user/delete', (req, res) => {
  if (!isDevAuthorized(req)) {
    return res.status(403).json({ success: false, message: 'Неверный пароль разработчика!' });
  }

  const { targetUsername } = req.body || {};
  if (!targetUsername) return res.status(400).json({ success: false, message: 'Не указан никнейм пользователя!' });

  const users = loadUsers();
  const targetLower = String(targetUsername).toLowerCase();
  let foundUid = null;

  for (const uid in users) {
    if (users[uid].username && users[uid].username.toLowerCase() === targetLower) {
      foundUid = uid;
      break;
    }
  }

  if (!foundUid) {
    return res.status(404).json({ success: false, message: 'Пользователь не найден!' });
  }

  deleteUser(foundUid);

  for (const token in activeSessions) {
    if (activeSessions[token].userId && activeSessions[token].userId.toLowerCase() === targetLower) {
      deleteSession(token);
    }
  }

  for (const [sId, uLower] of connectedUserSockets.entries()) {
    if (uLower === targetLower) {
      const socket = io.sockets.sockets.get(sId);
      if (socket) {
        socket.emit('accountDeactivated');
        socket.disconnect(true);
      }
    }
  }

  res.json({
    success: true,
    message: `Аккаунт @${targetUsername} навсегда удален из базы данных!`
  });
});

app.post(['/api/friends/add', '/api/friends/request/send'], (req, res) => {
  const authHeader = req.headers.authorization;
  let token = null;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7);
  }
  if (!token || !activeSessions[token]) {
    return res.status(401).json({ success: false, message: 'Неавторизован!' });
  }

  const session = activeSessions[token];
  const users = loadUsers();
  const user = users[session.userId];
  if (!user) return res.status(404).json({ success: false, message: 'Пользователь не найден!' });

  let { targetUsername } = req.body;
  if (!targetUsername) return res.status(400).json({ success: false, message: 'Введите логин пользователя!' });

  targetUsername = String(targetUsername).replace(/^@/, '').trim().toLowerCase();
  if (targetUsername === session.userId.toLowerCase() || targetUsername === (user.username || '').toLowerCase()) {
    return res.status(400).json({ success: false, message: 'Нельзя отправить заявку самому себе!' });
  }

  let friendUser = null;
  let friendId = null;
  for (const uid in users) {
    if (users[uid].username && users[uid].username.toLowerCase() === targetUsername) {
      friendUser = users[uid];
      friendId = uid;
      break;
    }
  }

  if (!friendUser) {
    return res.status(404).json({ success: false, message: 'Пользователь с таким никнеймом не найден!' });
  }

  if (!user.friends) user.friends = [];
  if (!friendUser.friends) friendUser.friends = [];
  if (!user.sentRequests) user.sentRequests = [];
  if (!user.receivedRequests) user.receivedRequests = [];
  if (!friendUser.sentRequests) friendUser.sentRequests = [];
  if (!friendUser.receivedRequests) friendUser.receivedRequests = [];

  if (user.friends.includes(friendId)) {
    return res.status(400).json({ success: false, message: 'Этот пользователь уже у вас в друзьях!' });
  }

  if (user.sentRequests.includes(friendId)) {
    return res.status(400).json({ success: false, message: 'Вы уже отправили заявку этому пользователю!' });
  }

  // If target has already sent us a request, accept it automatically!
  if (user.receivedRequests.includes(friendId)) {
    user.friends.push(friendId);
    friendUser.friends.push(session.userId);
    user.receivedRequests = user.receivedRequests.filter(id => id !== friendId);
    friendUser.sentRequests = friendUser.sentRequests.filter(id => id !== session.userId);

    saveUser(session.userId, user);
    saveUser(friendId, friendUser);

    return res.json({
      success: true,
      message: `Взаимная заявка! Пользователь @${friendUser.username} добавлен в друзья!`
    });
  }

  user.sentRequests.push(friendId);
  friendUser.receivedRequests.push(session.userId);

  saveUser(session.userId, user);
  saveUser(friendId, friendUser);

  res.json({
    success: true,
    message: `Заявка в друзья успешно отправлена пользователю @${friendUser.username}!`
  });
});

app.get('/api/friends/list', (req, res) => {
  const authHeader = req.headers.authorization;
  let token = null;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7);
  }
  if (!token || !activeSessions[token]) {
    return res.status(401).json({ success: false, message: 'Неавторизован!' });
  }

  const session = activeSessions[token];
  const users = loadUsers();
  const user = users[session.userId];
  if (!user) return res.status(404).json({ success: false, message: 'Пользователь не найден!' });

  const formatUser = (uId) => {
    const u = users[uId];
    if (!u) return null;
    return {
      userId: uId,
      username: u.username,
      displayName: u.displayName || u.username,
      avatar: u.avatar || "⚽",
      mmr: parseInt(u.user_mmr || (u.data && u.data.user_mmr) || 0, 10),
      selectedTitle: u.selectedTitle || "🌱 Новичок",
      isOnline: isUserOnline(u.username)
    };
  };

  const friendsList = (user.friends || []).map(formatUser).filter(Boolean);
  const sentRequests = (user.sentRequests || []).map(formatUser).filter(Boolean);
  const receivedRequests = (user.receivedRequests || []).map(formatUser).filter(Boolean);

  res.json({
    success: true,
    friends: friendsList,
    sentRequests: sentRequests,
    receivedRequests: receivedRequests
  });
});

app.post('/api/friends/request/accept', (req, res) => {
  const authHeader = req.headers.authorization;
  let token = null;
  if (authHeader && authHeader.startsWith('Bearer ')) token = authHeader.substring(7);
  if (!token || !activeSessions[token]) {
    return res.status(401).json({ success: false, message: 'Неавторизован!' });
  }

  const session = activeSessions[token];
  const users = loadUsers();
  const user = users[session.userId];
  if (!user) return res.status(404).json({ success: false, message: 'Пользователь не найден!' });

  let { targetUsername } = req.body;
  if (!targetUsername) return res.status(400).json({ success: false, message: 'Не указан никнейм!' });

  targetUsername = String(targetUsername).replace(/^@/, '').trim().toLowerCase();
  let friendId = null;
  let friendUser = null;
  for (const uid in users) {
    if (users[uid].username && users[uid].username.toLowerCase() === targetUsername) {
      friendId = uid;
      friendUser = users[uid];
      break;
    }
  }

  if (!friendId || !friendUser) {
    return res.status(404).json({ success: false, message: 'Пользователь не найден!' });
  }

  if (!user.friends) user.friends = [];
  if (!friendUser.friends) friendUser.friends = [];

  if (!user.friends.includes(friendId)) user.friends.push(friendId);
  if (!friendUser.friends.includes(session.userId)) friendUser.friends.push(session.userId);

  user.receivedRequests = (user.receivedRequests || []).filter(id => id !== friendId);
  user.sentRequests = (user.sentRequests || []).filter(id => id !== friendId);
  friendUser.sentRequests = (friendUser.sentRequests || []).filter(id => id !== session.userId);
  friendUser.receivedRequests = (friendUser.receivedRequests || []).filter(id => id !== session.userId);

  saveUser(session.userId, user);
  saveUser(friendId, friendUser);

  res.json({ success: true, message: `Заявка принята! Вы теперь друзья с @${friendUser.username}` });
});

app.post('/api/friends/request/reject', (req, res) => {
  const authHeader = req.headers.authorization;
  let token = null;
  if (authHeader && authHeader.startsWith('Bearer ')) token = authHeader.substring(7);
  if (!token || !activeSessions[token]) {
    return res.status(401).json({ success: false, message: 'Неавторизован!' });
  }

  const session = activeSessions[token];
  const users = loadUsers();
  const user = users[session.userId];
  if (!user) return res.status(404).json({ success: false, message: 'Пользователь не найден!' });

  let { targetUsername } = req.body;
  if (!targetUsername) return res.status(400).json({ success: false, message: 'Не указан никнейм!' });

  targetUsername = String(targetUsername).replace(/^@/, '').trim().toLowerCase();
  let friendId = null;
  let friendUser = null;
  for (const uid in users) {
    if (users[uid].username && users[uid].username.toLowerCase() === targetUsername) {
      friendId = uid;
      friendUser = users[uid];
      break;
    }
  }

  user.receivedRequests = (user.receivedRequests || []).filter(id => id !== friendId && id.toLowerCase() !== targetUsername);
  user.sentRequests = (user.sentRequests || []).filter(id => id !== friendId && id.toLowerCase() !== targetUsername);

  if (friendUser) {
    friendUser.sentRequests = (friendUser.sentRequests || []).filter(id => id !== session.userId);
    friendUser.receivedRequests = (friendUser.receivedRequests || []).filter(id => id !== session.userId);
    saveUser(friendId, friendUser);
  }

  saveUser(session.userId, user);

  res.json({ success: true, message: 'Заявка отменена/отклонена' });
});

app.post('/api/friends/remove', (req, res) => {
  const authHeader = req.headers.authorization;
  let token = null;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7);
  }
  if (!token || !activeSessions[token]) {
    return res.status(401).json({ success: false, message: 'Неавторизован!' });
  }

  const session = activeSessions[token];
  const users = loadUsers();
  const user = users[session.userId];
  if (!user) return res.status(404).json({ success: false, message: 'Пользователь не найден!' });

  const { targetUsername } = req.body;
  if (!targetUsername) return res.status(400).json({ success: false, message: 'Не указан логин друга!' });

  const targetLower = String(targetUsername).replace(/^@/, '').trim().toLowerCase();
  let friendId = null;
  let friendUser = null;

  for (const uid in users) {
    if (users[uid].username && users[uid].username.toLowerCase() === targetLower) {
      friendId = uid;
      friendUser = users[uid];
      break;
    }
  }

  if (user.friends) {
    user.friends = user.friends.filter(id => id.toLowerCase() !== targetLower && id !== friendId);
    saveUser(session.userId, user);
  }

  if (friendUser && friendUser.friends) {
    friendUser.friends = friendUser.friends.filter(id => id.toLowerCase() !== session.userId.toLowerCase() && id !== session.userId);
    saveUser(friendId, friendUser);
  }

  res.json({ success: true, message: 'Друг удален из списка!' });
});

app.post('/api/user/delete-account', (req, res) => {
  const authHeader = req.headers.authorization;
  let token = null;

  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7);
  }

  if (!token || !activeSessions[token]) {
    return res.status(401).json({ success: false, message: 'Неавторизован!' });
  }

  const session = activeSessions[token];
  const users = loadUsers();

  if (users[session.userId]) {
    deleteUser(session.userId);
  }

  deleteSession(token);

  res.json({ success: true, message: 'Учетная запись успешно удалена!' });
});

app.use((req, res, next) => {
  if (req.path === '/' || req.path === '/index.html' || req.path.endsWith('.png') || req.path.endsWith('.jpg')) {
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  }
  next();
});

app.use(express.static(__dirname));

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Real-time rooms state
const rooms = {};

// Matchmaking queues per goals count (1, 2, 3)
const matchmakingQueues = {
  1: [],
  2: [],
  3: []
};

function removeFromMatchmaking(socketId) {
  for (const goals in matchmakingQueues) {
    const queue = matchmakingQueues[goals];
    const index = queue.findIndex(item => item.socketId === socketId);
    if (index !== -1) {
      if (queue[index].timer) clearTimeout(queue[index].timer);
      queue.splice(index, 1);
    }
  }
}

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);

  socket.on('registerPresence', ({ username }) => {
    if (username) {
      connectedUserSockets.set(socket.id, String(username).toLowerCase());
    }
  });

  socket.on('joinMatchmaking', ({ goals, username, mmr, colorIdx, customPositions }) => {
    removeFromMatchmaking(socket.id);

    const goalKey = parseInt(goals, 10) || 2;
    if (!matchmakingQueues[goalKey]) matchmakingQueues[goalKey] = [];
    const queue = matchmakingQueues[goalKey];

    // Find waiting opponent
    const opponentIdx = queue.findIndex(item => item.socketId !== socket.id);

    if (opponentIdx !== -1) {
      const opponent = queue.splice(opponentIdx, 1)[0];
      if (opponent.timer) clearTimeout(opponent.timer);

      const roomCode = `mm_${Date.now()}_${Math.floor(Math.random() * 1000)}`;

      let p1Color = opponent.colorIdx !== undefined ? opponent.colorIdx : 0;
      let p2Color = colorIdx !== undefined ? colorIdx : 1;
      if (p1Color === p2Color) {
        p2Color = (p1Color + 1) % 11;
      }

      rooms[roomCode] = {
        id: roomCode,
        p1: { id: opponent.socketId, colorIdx: p1Color, ready: true, customPositions: opponent.customPositions || [] },
        p2: { id: socket.id, colorIdx: p2Color, ready: true, customPositions: customPositions || [] },
        settings: {
          maxGoalsToWin: goalKey,
          enableCoinToss: false,
          showCollisionBtn: true
        },
        state: 'GAME'
      };

      const socketP1 = io.sockets.sockets.get(opponent.socketId);
      if (socketP1) socketP1.join(roomCode);
      socket.join(roomCode);

      if (socketP1) {
        socketP1.emit('matchmakingFound', {
          room: roomCode,
          role: 'PLAYER1',
          opponentName: username || 'Игрок 2',
          opponentMmr: mmr || 0,
          goals: goalKey,
          p1ColorIdx: p1Color,
          p2ColorIdx: p2Color,
          p1CustomPositions: opponent.customPositions || [],
          p2CustomPositions: customPositions || []
        });
      }

      socket.emit('matchmakingFound', {
        room: roomCode,
        role: 'PLAYER2',
        opponentName: opponent.username || 'Игрок 1',
        opponentMmr: opponent.mmr || 0,
        goals: goalKey,
        p1ColorIdx: p1Color,
        p2ColorIdx: p2Color,
        p1CustomPositions: opponent.customPositions || [],
        p2CustomPositions: customPositions || []
      });

      console.log(`Matchmaking paired: ${opponent.username} vs ${username} in room ${roomCode}`);
    } else {
      const timer = setTimeout(() => {
        const idx = queue.findIndex(item => item.socketId === socket.id);
        if (idx !== -1) {
          queue.splice(idx, 1);
          socket.emit('matchmakingBotFallback', { goals: goalKey });
          console.log(`Matchmaking bot fallback sent to socket ${socket.id}`);
        }
      }, 10000);

      queue.push({
        socketId: socket.id,
        username: username || 'Игрок',
        mmr: mmr || 0,
        colorIdx: colorIdx !== undefined ? colorIdx : 0,
        customPositions: customPositions || [],
        timer: timer
      });

      console.log(`Socket ${socket.id} (${username}) queued for ${goalKey} goals matchmaking`);
    }
  });

  socket.on('cancelMatchmaking', () => {
    removeFromMatchmaking(socket.id);
    console.log(`Socket ${socket.id} cancelled matchmaking`);
  });

  socket.on('createRoom', ({ room, colorIdx, customPositions }) => {
    rooms[room] = {
      id: room,
      p1: { id: socket.id, colorIdx: colorIdx, ready: false, customPositions: customPositions || [] },
      p2: null,
      settings: {
        maxGoalsToWin: 3,
        enableCoinToss: true,
        showCollisionBtn: true
      },
      state: 'LOBBY'
    };
    socket.join(room);
    console.log(`Room created: ${room} by player ${socket.id}`);
    socket.emit('roomCreated', rooms[room]);
  });

  socket.on('joinRoom', ({ room, colorIdx, customPositions }) => {
    const r = rooms[room];
    if (!r) {
      socket.emit('errorMsg', 'Комната не найдена!');
      return;
    }
    if (r.p2) {
      socket.emit('errorMsg', 'Комната уже заполнена!');
      return;
    }
    let p2Color = colorIdx;
    if (r.p1 && p2Color === r.p1.colorIdx) {
      p2Color = (r.p1.colorIdx + 1) % 11;
    }
    r.p2 = { id: socket.id, colorIdx: p2Color, ready: false, customPositions: customPositions || [] };
    socket.join(room);
    console.log(`Player ${socket.id} joined room ${room}`);
    io.to(room).emit('roomJoined', r);
  });

  socket.on('changeColor', ({ room, colorIdx, role }) => {
    const r = rooms[room];
    if (!r) return;
    if (role === 'PLAYER1' && r.p1) {
      if (r.p2 && colorIdx === r.p2.colorIdx) return;
      r.p1.colorIdx = colorIdx;
    } else if (role === 'PLAYER2' && r.p2) {
      if (r.p1 && colorIdx === r.p1.colorIdx) return;
      r.p2.colorIdx = colorIdx;
    }
    io.to(room).emit('colorChanged', { p1ColorIdx: r.p1 ? r.p1.colorIdx : 0, p2ColorIdx: r.p2 ? r.p2.colorIdx : 1 });
  });

  socket.on('updateCustomTactic', ({ room, role, customPositions }) => {
    const r = rooms[room];
    if (!r) return;
    if (role === 'PLAYER1' && r.p1) {
      r.p1.customPositions = customPositions || [];
    } else if (role === 'PLAYER2' && r.p2) {
      r.p2.customPositions = customPositions || [];
    }
  });

  socket.on('updateSettings', ({ room, maxGoalsToWin, enableCoinToss, showCollisionBtn }) => {
    const r = rooms[room];
    if (!r) return;
    if (r.p1 && r.p1.id === socket.id) {
      r.settings = {
        maxGoalsToWin: Math.max(1, Math.min(5, parseInt(maxGoalsToWin) || 3)),
        enableCoinToss: !!enableCoinToss,
        showCollisionBtn: !!showCollisionBtn
      };
      io.to(room).emit('settingsUpdated', r.settings);
    }
  });

  socket.on('toggleReady', ({ room, role, ready, customPositions }) => {
    const r = rooms[room];
    if (!r) return;
    if (role === 'PLAYER1' && r.p1) {
      r.p1.ready = ready;
      if (customPositions) r.p1.customPositions = customPositions;
    } else if (role === 'PLAYER2' && r.p2) {
      r.p2.ready = ready;
      if (customPositions) r.p2.customPositions = customPositions;
    }
    io.to(room).emit('readyChanged', { p1Ready: r.p1 ? r.p1.ready : false, p2Ready: r.p2 ? r.p2.ready : false });

    // If both players are ready, start the match!
    if (r.p1 && r.p2 && r.p1.ready && r.p2.ready) {
      r.state = 'GAME';
      io.to(room).emit('matchStart', {
        p1ColorIdx: r.p1.colorIdx,
        p2ColorIdx: r.p2.colorIdx,
        p1CustomPositions: r.p1.customPositions || [],
        p2CustomPositions: r.p2.customPositions || [],
        settings: r.settings
      });
    }
  });

  socket.on('shoot', ({ room, index, forceX, forceY }) => {
    socket.to(room).emit('opponentShoot', { index, forceX, forceY });
  });

  socket.on('sync', ({ room, positions }) => {
    socket.to(room).emit('syncPositions', { positions });
  });

  socket.on('goalScored', ({ room, scoringTeam, nextTurnTeam, isAutoGoal, p1Goals, p2Goals }) => {
    const r = rooms[room];
    if (r) {
      r.skipVotes = {};
      io.to(room).emit('skipReplayStatus', { votes: 0 });
    }
    socket.to(room).emit('opponentGoalScored', { scoringTeam, nextTurnTeam, isAutoGoal, p1Goals, p2Goals });
  });

  socket.on('voteSkipReplay', ({ room, role }) => {
    const r = rooms[room];
    if (!r) return;
    if (!r.skipVotes) {
      r.skipVotes = {};
    }
    r.skipVotes[role] = true;
    const votesCount = Object.keys(r.skipVotes).length;
    
    io.to(room).emit('skipReplayStatus', { votes: votesCount });
    
    if (votesCount >= 2) {
      io.to(room).emit('forceSkipReplay');
      r.skipVotes = {};
    }
  });

  socket.on('rematch', ({ room }) => {
    const r = rooms[room];
    if (!r) return;
    if (r.p1) r.p1.ready = false;
    if (r.p2) r.p2.ready = false;
    r.state = 'LOBBY';
    io.to(room).emit('rematchTriggered');
  });

  socket.on('disconnect', () => {
    connectedUserSockets.delete(socket.id);
    removeFromMatchmaking(socket.id);
    console.log(`User disconnected: ${socket.id}`);
    for (const roomCode in rooms) {
      const r = rooms[roomCode];
      if (r.p1 && r.p1.id === socket.id) {
        io.to(roomCode).emit('opponentDisconnected');
        delete rooms[roomCode];
      } else if (r.p2 && r.p2.id === socket.id) {
        io.to(roomCode).emit('opponentDisconnected');
        r.p2 = null;
        r.state = 'LOBBY';
      }
    }
  });
});

httpServer.on('error', (err) => {
  console.error('HTTP server error (port ' + PORT + '):', err.message);
});

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});

if (CLOUD_RUN_PORT && CLOUD_RUN_PORT !== PORT) {
  try {
    const cloudRunServer = createServer(app);
    io.attach(cloudRunServer);
    cloudRunServer.on('error', (err) => {
      console.warn('Cloud Run secondary port warning:', err.message);
    });
    cloudRunServer.listen(CLOUD_RUN_PORT, '0.0.0.0', () => {
      console.log(`Cloud Run production listener running on port ${CLOUD_RUN_PORT}`);
    });
  } catch (err) {
    console.warn(`Could not start secondary listener on port ${CLOUD_RUN_PORT}:`, err.message);
  }
}
