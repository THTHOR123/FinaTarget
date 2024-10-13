const express = require('express');
const rateLimit = require('express-rate-limit');
const Queue = require('bull');
const fs = require('fs');
const Redis = require('ioredis');
require('dotenv').config();



// Initialize Redis client
const redisClient = new Redis();

//createing job queue
const taskQueue = new Queue('taskQueue', { redis: { port: 6379, host: '127.0.0.1' } });

const app = express();
app.use(express.json());

//checking redis connection error
redisClient.on('error', (err) => {
  console.error('Redis connection error:', err);
});


//limiter config
const taskLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 20,
  delayMs: 1000,
  keyGenerator: (req) => req.body.user_id || 'missing',
  handler: (req, res) => {
    if (!req.body.user_id) {
      return res.status(400).send('User ID is required.');
    }
    taskQueue.add({ user_id: req.body.user_id });
    res.status(429).send('Rate limit exceeded, task added to queue');
  },
});


//post route for the task
app.post("/api/v1/task", taskLimiter, (req, res) => {
  const userId = req.body.user_id;

  if (!userId || typeof userId !== 'string') {
    return res.status(400).send('Valid User ID is required.');
  }

  try {
    processTask(userId);
    return res.send(`Task for user ${userId} is being processed.`);
  } catch (error) {
    console.error('Error processing task:', error);
    return res.status(500).send('Internal Server Error');
  }
});


//function for processing the task and log completion
function processTask(user_id) {
  const log = `${user_id} - Task completed at - ${Date.now()}\n`;
  try {
    fs.appendFileSync('task.log', log);
  } catch (error) {
    console.error('Error writing to log:', error);
  }
  console.log(log);
}


//to process tasks from queue
taskQueue.process(async (task) => {
  try {
    await delay(1000);
    processTask(task.data.user_id);
  } catch (error) {
    console.error('task failed:', error);
    throw error; 
  }
});


//starting the server on port 3000
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
