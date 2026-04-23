const { Router } = require('express');

const router = Router();

router.get('/', (_req, res) => {
  res.json({ status: 'ok', message: 'Bot Dashboard Online' });
});

router.get('/health', (_req, res) => {
  res.json({ status: 'healthy' });
});

module.exports = router;
