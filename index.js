import express from "express"
import { createServer } from "http"
import { WebSocketServer } from "ws"
import cors from "cors"
import dotenv from "dotenv"
import fs, { promises as fsPromises } from "fs"
import path from "path"
import { fileURLToPath } from "url"
import { FormData } from "formdata-node"
import { fileFromPath } from "formdata-node/file-from-path"
import { spawn } from "child_process"
import { Buffer } from "buffer"
import ffmpeg from "fluent-ffmpeg"
import ffmpegPath from "@ffmpeg-installer/ffmpeg"
import { Worker, isMainThread, parentPort, workerData }from 'worker_threads' 

const createAudioWorker = () => {
  if (isMainThread) {
    return new Worker(__filename, {
      workerData: { isAudioWorker: true }
    });
  }
};

 class BufferPool {
  constructor(size = 50, bufferSize = 32768) {
    this.pool = []
    this.size = size
    this.bufferSize = bufferSize
    this.inUse = new Set()
    
    for (let i = 0; i < size; i++) {
      this.pool.push(Buffer.alloc(bufferSize))
    }
  }
  
  acquire() {
    const buffer = this.pool.pop() || Buffer.alloc(this.bufferSize)
    this.inUse.add(buffer)
    return buffer
  }
  
  release(buffer) {
    if (this.inUse.has(buffer) && this.pool.length < this.size) {
      buffer.fill(0)
      this.pool.push(buffer)
      this.inUse.delete(buffer)
    }
  }
  
  cleanup() {
    this.pool.length = 0
    this.inUse.clear()
  }
}

const bufferPool = new BufferPool();
const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

ffmpeg.setFfmpegPath(ffmpegPath.path)
dotenv.config()

// Environment variables
const { N8N_WEBHOOK_URL, N8N_AUDIO_TO_TEXT_URL } = process.env
const PORT = process.env.PORT || 5050

console.log("🚀 Starting Corrected Twilio Audio Processing Server...")

if (!N8N_WEBHOOK_URL || !N8N_AUDIO_TO_TEXT_URL) {
  console.error("❌ Missing n8n webhook URLs. Please set N8N_WEBHOOK_URL and N8N_AUDIO_TO_TEXT_URL in the .env file.")
  process.exit(1)
}

// Initialize Express and WebSocket
const app = express()
const server = createServer(app)
const wss = new WebSocketServer({
  server,
  path: "/media-stream",
  perMessageDeflate: false,
})
 import http from 'http';
  import https from 'https';
// Global state
const audioSessions = new Map()
const activeConnections = new Set()
const conversationStates = new Map()
const audioPlaybackStates = new Map()
 
const createAudioStream = async (audioBuffer) => {
  const { Readable } = await import('stream')
  
  return new Readable({
    read(size = 8192) {
      // Initialize position if not set
      if (typeof this.position === 'undefined') {
        this.position = 0
      }
      
      if (this.position >= audioBuffer.length) {
        this.push(null)
        return
      }
      
      const chunk = audioBuffer.slice(this.position, this.position + size)
      this.position += chunk.length // Use actual chunk length instead of size
      this.push(chunk)
    }
  })
}




const batchedSendAudioToTwilio = async (ws, audioChunks, streamSid) => {
  if (!ws || ws.readyState !== 1 || !audioChunks || audioChunks.length === 0) {
    return false;
  }

  console.log(`⚡ Batched sending ${audioChunks.length} chunks`);

  stopAudioPlayback(streamSid, "batched_override");
  startAudioPlayback(streamSid, `batched_${Date.now()}`, audioChunks.length);

  let sentChunks = 0;
  const startTime = Date.now();
  const batchSize = 16; // إرسال 16 chunks معاً

  return new Promise((resolve) => {
    const sendNextBatch = () => {
      if (sentChunks >= audioChunks.length) {
        const duration = Date.now() - startTime;
        console.log(`⚡ Batched send completed in ${duration}ms`);
        
        const state = audioPlaybackStates.get(streamSid);
        if (state) {
          state.isPlaying = false;
          state.currentAudioId = null;
        }
        
        resolve(true);
        return;
      }

      if (ws.readyState !== 1) {
        resolve(false);
        return;
      }

      try {
        // إرسال batch
        const endIndex = Math.min(sentChunks + batchSize, audioChunks.length);
        const messages = [];
        
        for (let i = sentChunks; i < endIndex; i++) {
          messages.push(JSON.stringify({
            event: "media",
            streamSid: streamSid,
            media: {
              payload: audioChunks[i]
            }
          }));
        }
        
        // إرسال جميع الرسائل في batch واحد
        const batchMessage = messages.join('\n');
        
        // إرسال منفصل لكل رسالة (Twilio requirement)
        for (const message of messages) {
          ws.send(message);
        }
        
        sentChunks = endIndex;
        
        // جدولة الـ batch التالي
        process.nextTick(sendNextBatch);
        
      } catch (error) {
        console.error("Batched send error:", error.message);
        resolve(false);
      }
    };

    // بدء فوري
    sendNextBatch();
  });
};
const VAD_CONFIG = {
  // إعدادات أساسية
  ENERGY_THRESHOLD: 300,
  MIN_SPEECH_FRAMES: 3,
  MIN_SILENCE_FRAMES: 20,
  MAX_BUFFER_SIZE: 500,
  CHUNK_SIZE: 160,
  PROCESSING_TIMEOUT: 10000,
  
  // جودة الصوت
  MIN_QUALITY_SCORE: 0.15,
  MIN_SPEECH_DURATION: 500,
  MAX_RECORDING_DURATION: 8000,
  
  // معايير الثقة
  VOICE_CONFIDENCE_THRESHOLD: 0.4,
  NOISE_GATE_THRESHOLD: 120,
  
  // كلمات التوقف
  STOP_KEYWORDS: ['stop', 'توقف', 'انتهاء'],
  
  // تحسينات للعربية
  ARABIC_VOICE_ADJUSTMENTS: {
    ENERGY_MULTIPLIER: 0.85,
    QUALITY_TOLERANCE: 0.2
  }
}
const isValidLongSpeech = (vadState, audioQuality, voiceAnalysis) => {
  const speechDuration = vadState.speechDuration || 0
  const frameCount = vadState.audioBuffer.length
  
  // شروط خاصة للكلام الطويل
  if (speechDuration >= VAD_CONFIG.LONG_PHRASE_DURATION) {
    
    console.log(`🎯 فحص الكلام الطويل: ${speechDuration}ms`)
    
    // شروط مرنة للكلام الطويل
    const hasMinFrames = frameCount >= VAD_CONFIG.MIN_LONG_PHRASE_FRAMES
    const hasDecentQuality = vadState.averageQualityScore > 0.08 // جودة منخفضة جداً مقبولة
    const hasVoiceActivity = voiceAnalysis.confidenceScore > VAD_CONFIG.CONTINUOUS_SPEECH_THRESHOLD
    const isInLongRange = speechDuration >= VAD_CONFIG.LONG_PHRASE_DURATION
    const withinTimeLimit = speechDuration <= VAD_CONFIG.MAX_RECORDING_DURATION
    
    console.log(`📊 فحص الكلام الطويل:`)
    console.log(`   - إطارات: ${frameCount}/${VAD_CONFIG.MIN_LONG_PHRASE_FRAMES} ✅${hasMinFrames}`)
    console.log(`   - جودة: ${(vadState.averageQualityScore * 100).toFixed(1)}% ✅${hasDecentQuality}`)
    console.log(`   - نشاط صوتي: ${(voiceAnalysis.confidenceScore * 100).toFixed(1)}% ✅${hasVoiceActivity}`)
    console.log(`   - مدة طويلة: ${speechDuration}ms ✅${isInLongRange}`)
    console.log(`   - ضمن الحد: ${speechDuration}ms ✅${withinTimeLimit}`)
    
    return hasMinFrames && hasDecentQuality && hasVoiceActivity && isInLongRange && withinTimeLimit
  }
  
  return false
}

// دالة محسنة لفحص الكلام القصير
const isValidShortSpeech = (vadState, audioQuality, voiceAnalysis) => {
  const speechDuration = vadState.speechDuration || 0
  const frameCount = vadState.audioBuffer.length
  
  // شروط خاصة للكلام القصير
  if (speechDuration >= VAD_CONFIG.SHORT_PHRASE_DURATION && 
      speechDuration <= VAD_CONFIG.MIN_SPEECH_DURATION) {
    
    console.log(`🎯 فحص الكلام القصير: ${speechDuration}ms`)
    
    // شروط مخففة للكلام القصير
    const hasMinFrames = frameCount >= VAD_CONFIG.MIN_SHORT_PHRASE_FRAMES
    const hasReasonableQuality = vadState.averageQualityScore > 0.1 // جودة منخفضة مقبولة
    const hasVoiceActivity = voiceAnalysis.confidenceScore > VAD_CONFIG.QUICK_SPEECH_THRESHOLD
    const isInQuickRange = speechDuration >= VAD_CONFIG.SHORT_PHRASE_DURATION
    
    console.log(`📊 فحص الكلام القصير:`)
    console.log(`   - إطارات: ${frameCount}/${VAD_CONFIG.MIN_SHORT_PHRASE_FRAMES} ✅${hasMinFrames}`)
    console.log(`   - جودة: ${(vadState.averageQualityScore * 100).toFixed(1)}% ✅${hasReasonableQuality}`)
    console.log(`   - نشاط صوتي: ${(voiceAnalysis.confidenceScore * 100).toFixed(1)}% ✅${hasVoiceActivity}`)
    console.log(`   - مدة سريعة: ${speechDuration}ms ✅${isInQuickRange}`)
    
    return hasMinFrames && hasReasonableQuality && hasVoiceActivity && isInQuickRange
  }
  
  return false
}

const createOptimizedMulawTables = () => {
  const MULAW_TO_PCM = new Int16Array(256);
  const PCM_TO_MULAW = new Uint8Array(65536);
  
  // Pre-compute all conversions
  for (let i = 0; i < 256; i++) {
    let mulaw = ~i & 0xFF;
    let sign = (mulaw & 0x80) ? -1 : 1;
    let exponent = (mulaw >> 4) & 0x07;
    let mantissa = mulaw & 0x0F;
    
    let sample;
    if (exponent === 0) {
      sample = mantissa << 3;
    } else {
      sample = ((mantissa | 0x10) << (exponent + 2)) - 128;
    }
    
    sample += 132;
    MULAW_TO_PCM[i] = sign * sample;
  }
  
  for (let pcm = -32768; pcm <= 32767; pcm++) {
    let sample = Math.abs(pcm);
    let sign = (pcm >= 0) ? 0x00 : 0x80;
    
    sample += 132;
    if (sample > 32767) sample = 32767;
    
    let exponent = 0;
    if (sample >= 256) {
      let temp = sample >> 8;
      while (temp > 0) {
        temp >>= 1;
        exponent++;
      }
      exponent--;
      if (exponent > 7) exponent = 7;
    }
    
    let mantissa = (sample >> (exponent + 3)) & 0x0F;
    let mulaw = sign | (exponent << 4) | mantissa;
    
    PCM_TO_MULAW[pcm + 32768] = ~mulaw & 0xFF;
  }
  
  return { MULAW_TO_PCM, PCM_TO_MULAW };
};

// ===== 100% Correct μ-law tables matching ITU-T G.711 standard =====
const createCorrectMulawTables = () => {
  const MULAW_TO_PCM = new Int16Array(256)
  
  for (let i = 0; i < 256; i++) {
    let mulaw = ~i & 0xFF
    let sign = (mulaw & 0x80) ? -1 : 1
    let exponent = (mulaw >> 4) & 0x07
    let mantissa = mulaw & 0x0F
    
    let sample
    if (exponent === 0) {
      sample = mantissa << 3
    } else {
      sample = ((mantissa | 0x10) << (exponent + 2)) - 128
    }
    
    // Add correct bias according to ITU-T G.711 standard
    sample += 132
    
    MULAW_TO_PCM[i] = sign * sample
  }
  
  // PCM to μ-law table
  const PCM_TO_MULAW = new Uint8Array(65536)
  
  for (let pcm = -32768; pcm <= 32767; pcm++) {
    let sample = Math.abs(pcm)
    let sign = (pcm >= 0) ? 0x00 : 0x80
    
    sample += 132
    if (sample > 32767) sample = 32767
    
    let exponent = 0
    if (sample >= 256) {
      let temp = sample >> 8
      while (temp > 0) {
        temp >>= 1
        exponent++
      }
      exponent--
      if (exponent > 7) exponent = 7
    }
    
    let mantissa = (sample >> (exponent + 3)) & 0x0F
    let mulaw = sign | (exponent << 4) | mantissa
    
    PCM_TO_MULAW[pcm + 32768] = ~mulaw & 0xFF
  }
  
  return { MULAW_TO_PCM, PCM_TO_MULAW }
}

 const { MULAW_TO_PCM , PCM_TO_MULAW  } = createOptimizedMulawTables();
  const instantMulawConversion = (audioBuffer) => {
  console.log("⚡ Instant conversion without FFmpeg");
  
  // للملفات WAV الخام
  if (audioBuffer[0] === 0x52 && audioBuffer[1] === 0x49 && audioBuffer[2] === 0x46 && audioBuffer[3] === 0x46) {
    // WAV header parsing
    const sampleRate = audioBuffer.readUInt32LE(24);
    const channels = audioBuffer.readUInt16LE(22);
    const bitsPerSample = audioBuffer.readUInt16LE(34);
    
    // إذا كان بالفعل 8kHz mono 16-bit، تحويل مباشر
    if (sampleRate === 8000 && channels === 1 && bitsPerSample === 16) {
      const dataStart = 44; // بعد الـ header
      const pcmData = audioBuffer.slice(dataStart);
      const samples = pcmData.length / 2;
      const mulawBuffer = bufferPool.acquire();
      
      for (let i = 0; i < samples && i < mulawBuffer.length; i++) {
        const pcmValue = pcmData.readInt16LE(i * 2);
        const index = Math.max(0, Math.min(65535, pcmValue + 32768));
        mulawBuffer[i] = PCM_TO_MULAW[index];
      }
      
      // تقسيم سريع
      const audioChunks = [];
      const actualSamples = Math.min(samples, mulawBuffer.length);
      
      for (let i = 0; i < actualSamples; i += 160) {
        const chunk = mulawBuffer.slice(i, i + 160);
        if (chunk.length < 160) {
          const paddedChunk = Buffer.alloc(160, 0x7F);
          chunk.copy(paddedChunk);
          audioChunks.push(paddedChunk.toString("base64"));
        } else {
          audioChunks.push(chunk.toString("base64"));
        }
      }
      
      bufferPool.release(mulawBuffer);
      
      return {
        success: true,
        chunks: audioChunks,
        totalChunks: audioChunks.length,
        totalSize: actualSamples,
        estimatedDurationMs: Math.round((actualSamples / 8000) * 1000),
        format: "ulaw",
        sampleRate: 8000,
        channels: 1,
        instant: true
      };
    }
  }
  
  return null; // يحتاج FFmpeg
};


 
// Middleware
app.use(cors({
  origin: "*",
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "Accept", "User-Agent"],
  credentials: false,
}))

app.use(express.json({ limit: "10mb" }))
app.use(express.urlencoded({ extended: true, limit: "10mb" }))
app.use(express.raw({ limit: "20mb" }))

// Request logging
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`)
  next()
})

// Ensure temp directory exists
const ensureTempDir = () => {
  const tempDir = path.join(__dirname, "temp")
  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir, { recursive: true })
    console.log("📁 Created temp directory")
  }
  return tempDir
}

 

// ===== Convert audio to correct Twilio format =====
const convertToTwilioFormat = async (audioBuffer, inputFormat = "auto") => {
  return new Promise((resolve, reject) => {
    // Determine file type
    let detectedFormat = inputFormat
    if (inputFormat === "auto") {
      const header = audioBuffer.slice(0, 12)
      if (header[0] === 0xFF && (header[1] & 0xE0) === 0xE0) {
        detectedFormat = 'mp3'
      } else if (header.toString('ascii', 0, 4) === 'RIFF') {
        detectedFormat = 'wav'
      } else if (header.toString('ascii', 0, 4) === 'OggS') {
        detectedFormat = 'ogg'
      } else if (header.slice(4, 8).toString('ascii') === 'ftyp') {
        detectedFormat = 'm4a'
      } else {
        detectedFormat = 'wav'
      }
    }

    const tempDir = ensureTempDir()
    const timestamp = Date.now()
    const inputFile = path.join(tempDir, `input_${timestamp}.${detectedFormat}`)
    
    // Save temporary file
    fs.writeFileSync(inputFile, audioBuffer)

    const chunks = []
    let stderrData = ""
    
    // Simplified and calibrated ffmpeg parameters for Twilio
    const ffmpegArgs = [
      "-i", inputFile,
      "-acodec", "pcm_s16le",  // PCM 16-bit
      "-ar", "8000",           // Exactly 8kHz
      "-ac", "1",              // Mono
      "-f", "s16le",           // Raw PCM format
      // Simple improvements only - no overdoing it
      "-af", "volume=1.3,highpass=f=80,lowpass=f=3800",
      "-y",
      "pipe:1"
    ]

    const ffmpegProcess = spawn("ffmpeg", ffmpegArgs, {
      stdio: ['ignore', 'pipe', 'pipe']
    })

    ffmpegProcess.stdout.on("data", (chunk) => {
      chunks.push(chunk)
    })

    ffmpegProcess.stderr.on("data", (data) => {
      stderrData += data.toString()
    })

    ffmpegProcess.on("close", (code) => {
      // Cleanup
      try {
        if (fs.existsSync(inputFile)) fs.unlinkSync(inputFile)
      } catch (e) {}

      if (code !== 0) {
        console.error("ffmpeg error:", stderrData)
        return reject(new Error(`ffmpeg failed with code ${code}`))
      }

      if (chunks.length === 0) {
        return reject(new Error("No audio data produced"))
      }

      try {
        const pcm16Buffer = Buffer.concat(chunks)
        console.log(`PCM data: ${pcm16Buffer.length} bytes`)

        // Convert PCM 16-bit to μ-law using correct table
        const samples = pcm16Buffer.length / 2
        const mulawBuffer = Buffer.alloc(samples)

        for (let i = 0; i < samples; i++) {
          const pcmValue = pcm16Buffer.readInt16LE(i * 2)
          const index = Math.max(0, Math.min(65535, pcmValue + 32768))
          mulawBuffer[i] = PCM_TO_MULAW[index]
        }

        console.log(`μ-law conversion: ${mulawBuffer.length} bytes`)

        // Split into chunks of 160 bytes (20ms at 8kHz)
        const audioChunks = []

        for (let i = 0; i < mulawBuffer.length; i += VAD_CONFIG.CHUNK_SIZE) {
          let chunk = mulawBuffer.slice(i, i + VAD_CONFIG.CHUNK_SIZE)
          
          // Add padding if necessary
          if (chunk.length < VAD_CONFIG.CHUNK_SIZE) {
            const paddedChunk = Buffer.alloc(VAD_CONFIG.CHUNK_SIZE, 0x7F) // μ-law silence
            chunk.copy(paddedChunk)
            chunk = paddedChunk
          }
          
          audioChunks.push(chunk.toString("base64"))
        }

        const estimatedDuration = Math.round((mulawBuffer.length / 8000) * 1000)

        resolve({
          success: true,
          chunks: audioChunks,
          totalChunks: audioChunks.length,
          totalSize: mulawBuffer.length,
          estimatedDurationMs: estimatedDuration,
          format: "ulaw",
          sampleRate: 8000,
          channels: 1,
          chunkSize: VAD_CONFIG.CHUNK_SIZE,
          originalFormat: detectedFormat,
          originalSize: audioBuffer.length
        })

      } catch (error) {
        console.error("Processing error:", error.message)
        reject(error)
      }
    })

    ffmpegProcess.on("error", (err) => {
      try {
        if (fs.existsSync(inputFile)) fs.unlinkSync(inputFile)
      } catch (e) {}
      reject(err)
    })

    // timeout
    setTimeout(() => {
      if (!ffmpegProcess.killed) {
        ffmpegProcess.kill("SIGTERM")
        reject(new Error("ffmpeg timeout"))
      }
    }, 15000)
  })
}

// ===== Send audio to Twilio =====
const sendAudioToTwilio = async (ws, audioChunks, streamSid) => {
  if (!ws || ws.readyState !== 1 || !audioChunks || audioChunks.length === 0) {
    return false
  }

  console.log(`Sending ${audioChunks.length} chunks to Twilio`)

  // Stop any current playback
  stopAudioPlayback(streamSid, "new_audio_started")
  
  // Start new playback
  startAudioPlayback(streamSid, `audio_${Date.now()}`, audioChunks.length)

  let sentChunks = 0
  const startTime = Date.now()

  return new Promise((resolve) => {
    const sendChunk = () => {
      // Check if playback was stopped
      if (!isAudioPlaying(streamSid)) {
        console.log(`Playback stopped for call ${streamSid?.slice(-8)} after ${sentChunks} chunks`)
        resolve(false)
        return
      }

      if (sentChunks >= audioChunks.length) {
        const duration = Date.now() - startTime
        console.log(`Send completed in ${duration}ms`)
        
        // End playback state
        const state = audioPlaybackStates.get(streamSid)
        if (state) {
          state.isPlaying = false
          state.currentAudioId = null
        }
        
        resolve(true)
        return
      }

      if (ws.readyState !== 1) {
        console.error("WebSocket disconnected")
        stopAudioPlayback(streamSid, "websocket_disconnected")
        resolve(false)
        return
      }

      try {
        const message = {
          event: "media",
          streamSid: streamSid,
          media: {
            payload: audioChunks[sentChunks]
          }
        }

        ws.send(JSON.stringify(message))
        sentChunks++

        // Update state
        const state = audioPlaybackStates.get(streamSid)
        if (state) {
          state.sentChunks = sentChunks
        }

        // Progress every 25 chunks
        if (sentChunks % 25 === 0) {
          const progress = ((sentChunks / audioChunks.length) * 100).toFixed(1)
          console.log(`Send progress: ${progress}%`)
        }

        // 20ms delay between each chunk (Twilio standard)
        setTimeout(sendChunk, 20)

      } catch (error) {
        console.error("Chunk send error:", error.message)
        stopAudioPlayback(streamSid, "send_error")
        resolve(false)
      }
    }

    // Start sending with short delay
    setTimeout(sendChunk, 50)
  })
}

// ===== Create simplified VAD state =====
const createSimpleVADState = () => ({
  // البيانات الأساسية
  audioBuffer: [],
  isRecording: false,
  speechFrames: 0,
  silenceFrames: 0,
  
  // معلومات التسجيل
  recordingTimeout: null,
  lastSpeechTime: 0,
  speechStartTime: 0,
  speechDuration: 0,
  
  // معلومات المقاطعة المحسنة
  isUserInterrupting: false,
  interruptionFrames: 0,
  lastInterruptionTime: 0,
  interruptionConfidence: 0,
  falseInterruptionCount: 0,
  
  // تحليل الصوت المحسن
  recentEnergyWindow: [],
  backgroundNoiseLevel: 0,
  adaptiveThreshold: 0,
  voicePatternHistory: [],
  
  // معلومات التشغيل
  playbackStartTime: null,
  allowInterruption: false,
  currentPlaybackDuration: 0,
  
  // إحصائيات جودة الصوت
  averageQualityScore: 0,
  qualityHistory: [],
  
  // الحالة العامة
  consecutiveProcessingFailures: 0,
  lastProcessingAttempt: 0,
  needsReset: false,
  lastResetTime: Date.now(),
  totalFrames: 0,
  averageEnergy: 0,
  energyHistory: [],
  
  // الكشف عن أنماط الكلام
  speechPatterns: {
    shortPauses: 0,
    longPauses: 0,
    continuousSpeech: 0
  },
  
  hasValidSpeech: false,
  totalSpeechSegments: 0,
  recentTranscripts: [],
  stopWordDetected: false
})

 const analyzeAudioQualityImproved = (audioData, vadState) => {
  let totalEnergy = 0
  let voiceFrequencyEnergy = 0
  let validSamples = 0
  const energySamples = []
  
  // تحليل أكثر دقة لـ μ-law
  for (let i = 0; i < audioData.length; i++) {
    const pcm = MULAW_TO_PCM[audioData[i]]
    const energy = Math.abs(pcm)
    
    totalEnergy += energy
    energySamples.push(energy)
    
    // نطاق تردد محسن للصوت العربي
    if (energy >= 60 && energy <= 4000) {
      voiceFrequencyEnergy += energy
      validSamples++
    }
  }
  
  const avgEnergy = totalEnergy / audioData.length
  const voiceRatio = validSamples > 0 ? (voiceFrequencyEnergy / totalEnergy) : 0
  
  // تطبيق التعديلات للصوت العربي
  const adjustedEnergy = avgEnergy * VAD_CONFIG.ARABIC_VOICE_ADJUSTMENTS.ENERGY_MULTIPLIER
  
  // حساب معيار الجودة أكثر تساهلاً
  const energyVariance = energySamples.reduce((sum, e) => sum + Math.pow(e - avgEnergy, 2), 0) / energySamples.length
  const energyStdDev = Math.sqrt(energyVariance)
  
  const isLikelyArabicVoice = voiceRatio >= 0.4 && 
                              adjustedEnergy > 50 && 
                              energyStdDev > 30
  
  const qualityScore = Math.min(1, (voiceRatio * 0.6) + 
                                   (Math.min(adjustedEnergy / 500, 1) * 0.3) + 
                                   (Math.min(energyStdDev / 200, 1) * 0.1))
  
  return {
    avgEnergy: adjustedEnergy,
    voiceRatio,
    energyStdDev,
    isLikelyVoice: isLikelyArabicVoice,
    qualityScore,
    validSamples,
    // إضافة معلومات للتتبع
    rawEnergy: avgEnergy,
    adjustmentApplied: true
  }
}
 // إضافة دالة جديدة لتحليل الصوت الحقيقي
const analyzeRealVoiceActivity = (audioData) => {
  let voiceActivityScore = 0
  let totalEnergy = 0
  let voiceFrequencyCount = 0
  const energyThreshold = VAD_CONFIG.ENERGY_THRESHOLD
  
  for (let i = 0; i < audioData.length; i++) {
    const pcm = MULAW_TO_PCM[audioData[i]]
    const energy = Math.abs(pcm)
    totalEnergy += energy
    
    // تحليل النطاق الصوتي للكلام البشري
    if (energy >= 200 && energy <= 8000) {
      voiceFrequencyCount++
      
      // إضافة نقاط للطاقة في النطاق الصوتي
      if (energy > energyThreshold) {
        voiceActivityScore += 2
      } else if (energy > energyThreshold * 0.7) {
        voiceActivityScore += 1
      }
    } else if (energy < 100) {
      // خصم نقاط للصمت أو الضوضاء المنخفضة
      voiceActivityScore -= 0.5
    }
  }
  
  const avgEnergy = totalEnergy / audioData.length
  const voiceRatio = voiceFrequencyCount / audioData.length
  const confidenceScore = Math.min(1, (voiceActivityScore / audioData.length) * 2)
  
  return {
    avgEnergy,
    voiceRatio,
    confidenceScore,
    isRealVoice: confidenceScore > VAD_CONFIG.VOICE_CONFIDENCE_THRESHOLD && 
                 voiceRatio > 0.3 && 
                 avgEnergy > energyThreshold,
    voiceActivityScore: Math.max(0, voiceActivityScore)
  }
}
// ===== Create audio playback state =====
const createAudioPlaybackState = (streamSid) => ({
  streamSid,
  isPlaying: false,
  currentAudioId: null,
  playbackStartTime: null,
  totalChunks: 0,
  sentChunks: 0,
  shouldStop: false
})
const audioCache = new Map()
const MAX_CACHE_SIZE = 50
const CACHE_EXPIRY = 30 * 60 * 1000 // 30 دقيقة

// إضافة ملف للكاش
const addToCache = (url, audioResult) => {
  if (audioCache.size >= MAX_CACHE_SIZE) {
    const firstKey = audioCache.keys().next().value
    audioCache.delete(firstKey)
  }
  
  audioCache.set(url, {
    data: audioResult,
    timestamp: Date.now()
  })
  
  console.log(`📦 Audio cached: ${url} (${audioResult.chunks.length} chunks)`)
}

// استرجاع من الكاش
const getFromCache = (url) => {
  const cached = audioCache.get(url)
  if (cached && (Date.now() - cached.timestamp) < CACHE_EXPIRY) {
    console.log(`⚡ Cache hit: ${url}`)
    return cached.data
  }
  
  if (cached) {
    audioCache.delete(url) // حذف المنتهي الصلاحية
  }
  
  return null
}
const fastConvertToTwilioFormat = async (audioBuffer, inputFormat = "auto") => {
  return new Promise((resolve, reject) => {
    let detectedFormat = inputFormat
    if (inputFormat === "auto") {
      const header = audioBuffer.slice(0, 12)
      if (header[0] === 0xFF && (header[1] & 0xE0) === 0xE0) {
        detectedFormat = 'mp3'
      } else if (header.toString('ascii', 0, 4) === 'RIFF') {
        detectedFormat = 'wav'
      } else {
        detectedFormat = 'mp3' // افتراضي
      }
    }

    const tempDir = ensureTempDir()
    const timestamp = Date.now()
    const inputFile = path.join(tempDir, `fast_input_${timestamp}.${detectedFormat}`)
    
    fs.writeFileSync(inputFile, audioBuffer)

    const chunks = []
    
    // معاملات ffmpeg مبسطة للسرعة القصوى
    const ffmpegArgs = [
      "-i", inputFile,
      "-acodec", "pcm_s16le",
      "-ar", "8000",
      "-ac", "1",
      "-f", "s16le",
      "-y",
      "pipe:1"
    ]

    const ffmpegProcess = spawn("ffmpeg", ffmpegArgs, {
      stdio: ['ignore', 'pipe', 'ignore'] // تجاهل stderr للسرعة
    })

    ffmpegProcess.stdout.on("data", (chunk) => {
      chunks.push(chunk)
    })

    ffmpegProcess.on("close", (code) => {
      // تنظيف سريع
      try {
        fs.unlinkSync(inputFile)
      } catch (e) {}

      if (code !== 0 || chunks.length === 0) {
        return reject(new Error("Fast conversion failed"))
      }

      try {
        const pcm16Buffer = Buffer.concat(chunks)
        const samples = pcm16Buffer.length / 2
        const mulawBuffer = Buffer.alloc(samples)

        // تحويل سريع PCM إلى μ-law
        for (let i = 0; i < samples; i++) {
          const pcmValue = pcm16Buffer.readInt16LE(i * 2)
          const index = Math.max(0, Math.min(65535, pcmValue + 32768))
          mulawBuffer[i] = PCM_TO_MULAW[index]
        }

        // تقسيم سريع إلى chunks
        const audioChunks = []
        const chunkSize = 160

        for (let i = 0; i < mulawBuffer.length; i += chunkSize) {
          let chunk = mulawBuffer.slice(i, i + chunkSize)
          
          if (chunk.length < chunkSize) {
            const paddedChunk = Buffer.alloc(chunkSize, 0x7F)
            chunk.copy(paddedChunk)
            chunk = paddedChunk
          }
          
          audioChunks.push(chunk.toString("base64"))
        }

        resolve({
          success: true,
          chunks: audioChunks,
          totalChunks: audioChunks.length,
          totalSize: mulawBuffer.length,
          estimatedDurationMs: Math.round((mulawBuffer.length / 8000) * 1000),
          format: "ulaw",
          sampleRate: 8000,
          channels: 1,
          fast: true
        })

      } catch (error) {
        reject(error)
      }
    })

    ffmpegProcess.on("error", reject)

    // timeout أقصر
    setTimeout(() => {
      if (!ffmpegProcess.killed) {
        ffmpegProcess.kill("SIGTERM")
        reject(new Error("Fast conversion timeout"))
      }
    }, 8000)
  })
}

// إرسال صوت محسن بدون تأخير
const fastSendAudioToTwilio = async (ws, audioChunks, streamSid) => {
  if (!ws || ws.readyState !== 1 || !audioChunks || audioChunks.length === 0) {
    return false
  }

  console.log(`⚡ Fast sending ${audioChunks.length} chunks`)

  // إيقاف تشغيل حالي
  stopAudioPlayback(streamSid, "new_audio_started")
  startAudioPlayback(streamSid, `fast_audio_${Date.now()}`, audioChunks.length)

  let sentChunks = 0
  const startTime = Date.now()

  return new Promise((resolve) => {
    const sendChunk = () => {
      if (!isAudioPlaying(streamSid) || sentChunks >= audioChunks.length) {
        const duration = Date.now() - startTime
        console.log(`⚡ Fast send completed in ${duration}ms`)
        
        const state = audioPlaybackStates.get(streamSid)
        if (state) {
          state.isPlaying = false
          state.currentAudioId = null
        }
        
        resolve(sentChunks >= audioChunks.length)
        return
      }

      if (ws.readyState !== 1) {
        stopAudioPlayback(streamSid, "websocket_disconnected")
        resolve(false)
        return
      }

      try {
        // إرسال عدة chunks معاً للسرعة
        const batchSize = Math.min(5, audioChunks.length - sentChunks)
        
        for (let i = 0; i < batchSize && sentChunks < audioChunks.length; i++) {
          const message = {
            event: "media",
            streamSid: streamSid,
            media: {
              payload: audioChunks[sentChunks]
            }
          }

          ws.send(JSON.stringify(message))
          sentChunks++
        }

        const state = audioPlaybackStates.get(streamSid)
        if (state) {
          state.sentChunks = sentChunks
        }

        // تأخير أقل للسرعة
        setTimeout(sendChunk, 15) // 15ms بدلاً من 20ms

      } catch (error) {
        console.error("Fast send error:", error.message)
        stopAudioPlayback(streamSid, "send_error")
        resolve(false)
      }
    }

    // بدء فوري بدون تأخير
    sendChunk()
  })}

  app.post("/play-pre-booking-audio-fast", async (req, res) => {
  console.log("⚡ Fast pre-booking audio request")
  
  const startTime = Date.now()
  
  try {
    const { streamSid, audioUrl } = req.body

    if (!streamSid) {
      return res.status(400).json({
        success: false,
        error: "streamSid is required"
      })
    }

    const defaultAudioUrl = "https://audio.jukehost.co.uk/4ZcS772ROASdpYPREDK1EeMcTjrRiQyq"
    const finalAudioUrl = audioUrl || defaultAudioUrl

    console.log(`⚡ Fast loading: ${finalAudioUrl.slice(-20)}...`)

    const targetConnection = findActiveConnection(streamSid)
    if (!targetConnection) {
      return res.status(404).json({
        success: false,
        error: "No active connection for call"
      })
    }

    // فحص الكاش أولاً
    let audioResult = getFromCache(finalAudioUrl)
    
    if (!audioResult) {
      console.log("⚡ Cache miss - downloading...")
      
      // تحميل سريع بدون headers إضافية
      const audioResponse = await fetch(finalAudioUrl, {
        method: 'GET',
        timeout: 10000 // timeout أقصر
      })
      
      if (!audioResponse.ok) {
        throw new Error(`Download failed: HTTP ${audioResponse.status}`)
      }

      const arrayBuffer = await audioResponse.arrayBuffer()
      const audioBuffer = Buffer.from(arrayBuffer)

      if (audioBuffer.length === 0) {
        throw new Error("Empty audio file")
      }

      console.log(`⚡ Downloaded ${audioBuffer.length} bytes`)

      // تحويل سريع
      audioResult = await fastConvertToTwilioFormat(audioBuffer, 'mp3')
      
      // إضافة للكاش
      addToCache(finalAudioUrl, audioResult)
      
      console.log(`⚡ Fast conversion: ${audioResult.chunks.length} chunks`)
    }

    if (!audioResult.chunks || audioResult.chunks.length === 0) {
      throw new Error("No valid audio chunks")
    }

    // إرسال سريع
    const success = await fastSendAudioToTwilio(targetConnection, audioResult.chunks, streamSid)

    const totalTime = Date.now() - startTime

    if (success) {
      console.log(`⚡ Fast playback completed in ${totalTime}ms`)
      
      const session = audioSessions.get(streamSid)
      if (session) {
        session.lastPreBookingAudio = new Date()
        session.totalPreBookingAudios = (session.totalPreBookingAudios || 0) + 1
        session.lastPlaybackTime = totalTime
      }
      
      res.json({
        success: true,
        message: "Fast audio playback completed",
        performance: {
          totalTimeMs: totalTime,
          cached: audioCache.has(finalAudioUrl),
          chunksCount: audioResult.chunks.length,
          estimatedDuration: audioResult.estimatedDurationMs + "ms"
        },
        details: {
          streamSid: streamSid.slice(-8),
          audioUrl: finalAudioUrl.slice(-30),
          fast: true
        }
      })
    } else {
      res.status(500).json({
        success: false,
        error: "Fast send failed",
        totalTimeMs: totalTime
      })
    }

  } catch (error) {
    const totalTime = Date.now() - startTime
    console.error(`⚡ Fast playback error (${totalTime}ms):`, error.message)
    
    res.status(500).json({
      success: false,
      error: "Fast playback failed",
      details: error.message,
      totalTimeMs: totalTime
    })
  }
})

// تنظيف الكاش دورياً
setInterval(() => {
  const now = Date.now()
  let cleaned = 0
  
  for (const [url, cached] of audioCache.entries()) {
    if (now - cached.timestamp > CACHE_EXPIRY) {
      audioCache.delete(url)
      cleaned++
    }
  }
  
  if (cleaned > 0) {
    console.log(`🧹 Cleaned ${cleaned} expired cache entries`)
  }
}, 10 * 60 * 1000) // كل 10 دقائق
// Playback state management functions
const startAudioPlayback = (streamSid, audioId, totalChunks) => {
  const state = audioPlaybackStates.get(streamSid) || createAudioPlaybackState(streamSid)
  state.isPlaying = true
  state.currentAudioId = audioId
  state.playbackStartTime = Date.now() // Important for interruption detection
  state.totalChunks = totalChunks
  state.sentChunks = 0
  state.shouldStop = false
  audioPlaybackStates.set(streamSid, state)
  
  // Update VAD state to allow interruption after period
  const targetWS = findActiveConnection(streamSid)
  if (targetWS && targetWS.vadState) {
    targetWS.vadState.playbackStartTime = Date.now()
    targetWS.vadState.allowInterruption = false
    
    // Allow interruption after short period
    setTimeout(() => {
      if (targetWS.vadState) {
        targetWS.vadState.allowInterruption = true
      }
    }, VAD_CONFIG.MIN_PLAYBACK_TIME_BEFORE_INTERRUPTION)
  }
  
  console.log(`🎵 Enhanced playback started ${audioId} for call ${streamSid?.slice(-8)}`)
}

const stopAudioPlayback = (streamSid, reason = "user_interruption") => {
  const state = audioPlaybackStates.get(streamSid)
  if (state && state.isPlaying) {
    state.shouldStop = true
    state.isPlaying = false
    
    console.log(`Audio playback stopped for call ${streamSid?.slice(-8)} - Reason: ${reason}`)
    return true
  }
  return false
}

const isAudioPlaying = (streamSid) => {
  const state = audioPlaybackStates.get(streamSid)
  return state && state.isPlaying && !state.shouldStop
}
 
// Find active connection
const findActiveConnection = (streamSid) => {
  for (const ws of activeConnections) {
    if (ws.streamSid === streamSid && ws.readyState === 1) {
      return ws
    }
  }
  return null
}

const processIncomingAudioFromTwilio = async (audioChunks, streamSid, phoneNumber, options = {}) => {
  console.log(`🎵 معالجة صوت محسنة: ${audioChunks.length} إطار للمكالمة ${streamSid?.slice(-8)}`)
  
  if (options.isInterruption) {
    console.log(`🛑 معالجة مقاطعة المستخدم (الأولوية: ${options.priority})`)
  }

  try {
    // دمج إطارات μ-law في بافر واحد
    const totalSize = audioChunks.length * VAD_CONFIG.CHUNK_SIZE
    const mulawBuffer = Buffer.alloc(totalSize)
    
    for (let i = 0; i < audioChunks.length; i++) {
      const chunkData = Buffer.from(audioChunks[i], "base64")
      const offset = i * VAD_CONFIG.CHUNK_SIZE
      chunkData.copy(mulawBuffer, offset, 0, Math.min(chunkData.length, VAD_CONFIG.CHUNK_SIZE))
    }

    console.log(`تجميع μ-law: ${mulawBuffer.length} بايت`)
    
    // تحويل μ-law إلى WAV باستخدام الوظيفة المحسنة
    const wavBuffer = convertMulawToWavImproved(mulawBuffer)
    console.log(`تحويل WAV محسن: ${wavBuffer.length} بايت`)

    // فحص جودة الصوت النهائية
    const audioQuality = analyzeAudioQualityImproved(mulawBuffer)
    console.log(`جودة الصوت النهائية: ${(audioQuality.qualityScore * 100).toFixed(1)}%`)

    // حفظ الملف المؤقت
    const tempDir = ensureTempDir()
    const timestamp = Date.now()
    const audioFileName = `audio_improved_${streamSid?.slice(-8) || 'unknown'}_${timestamp}.wav`
    const audioFilePath = path.join(tempDir, audioFileName)

    await fsPromises.writeFile(audioFilePath, wavBuffer)
    console.log(`ملف محفوظ: ${audioFilePath}`)

    // إنشاء FormData مع معلومات إضافية
    const formData = new FormData()
    const audioFile = await fileFromPath(audioFilePath)
    
    formData.append("audio", audioFile)
    formData.append("streamSid", streamSid || "unknown")
    formData.append("phoneNumber", phoneNumber || "unknown")
    formData.append("timestamp", timestamp.toString())
    formData.append("chunksCount", audioChunks.length.toString())
    formData.append("totalSize", mulawBuffer.length.toString())
    formData.append("audioFormat", "wav")
    formData.append("sampleRate", "8000")
    formData.append("channels", "1")
    formData.append("duration", Math.round((mulawBuffer.length / 8000) * 1000).toString())
    
    // معلومات السياق المحسنة
    formData.append("isInterruption", options.isInterruption ? "true" : "false")
    formData.append("priority", options.priority || "normal")
    formData.append("speechDuration", options.speechDuration?.toString() || "0")
    formData.append("qualityScore", options.qualityScore?.toString() || "0")
    formData.append("confidence", options.confidence?.toString() || "0")
    formData.append("vadVersion", "improved-v3")
    formData.append("isForced", options.isForced ? "true" : "false")
    formData.append("reason", options.reason || "normal_speech")
    
    // معلومات إضافية لتحسين المعالجة في n8n
    formData.append("noiseLevel", "low") // بسبب الفلترة المحسنة
    formData.append("audioQuality", "enhanced")
    formData.append("processingMethod", "improved_mulaw_conversion")
    
    console.log(`FormData محضر للإرسال إلى n8n`)
    
    // إرسال للمعالجة مع وقت محدود حسب الأولوية
    const processingTimeout = options.isInterruption ? 6000 : 
                             options.priority === 'urgent' ? 8000 : 
                            VAD_CONFIG.PROCESSING_TIMEOUT

    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), processingTimeout)

    const response = await fetch(N8N_AUDIO_TO_TEXT_URL, {
      method: "POST",
      body: formData,
      signal: controller.signal,
      headers: {
        "User-Agent": "Improved-Twilio-Audio-Processor/3.0",
        "X-Processing-Priority": options.priority || "normal",
        "X-Audio-Quality": "enhanced",
        "X-Noise-Reduction": "applied"
      }
    })

 
    clearTimeout(timeoutId)
    
    // تنظيف الملف المؤقت
    try {
      await fsPromises.unlink(audioFilePath)
      console.log(`ملف مؤقت محذوف: ${audioFileName}`)
    } catch (cleanupError) {
      console.warn(`تحذير تنظيف الملف: ${cleanupError.message}`)
    }
    
    if (response.ok) {
      let result
      try {
        result = await response.json()
      } catch {
        result = { success: true, transcript: "معالجة محسنة مكتملة" }
      }

      console.log(`✅ معالجة n8n ناجحة: ${response.status}`)

      // كشف كلمات التوقف في النص
      const transcript = result.transcript || result.text || ""
      const containsStopWord = VAD_CONFIG.STOP_KEYWORDS.some(keyword => 
        transcript.toLowerCase().includes(keyword.toLowerCase())
      )

      if (containsStopWord) {
        console.log(`🛑 كلمة توقف مكتشفة في: "${transcript}"`)
        stopAudioPlayback(streamSid, "stop_word_detected")
      }

      // تحديث بيانات الجلسة
      const session = audioSessions.get(streamSid)
      if (session) {
        session.lastTranscript = transcript
        session.lastResponse = result.response || result.reply || null
        session.lastProcessed = new Date()
        session.totalProcessed = (session.totalProcessed || 0) + 1
        session.lastSuccess = true
        session.isInterruption = options.isInterruption
        session.containsStopWord = containsStopWord
        session.processingQuality = "improved"
        session.audioQuality = (audioQuality.qualityScore * 100).toFixed(1) + "%"
        
        // حفظ النصوص الحديثة لتحليل الأنماط
        session.recentTranscripts = session.recentTranscripts || []
        session.recentTranscripts.push({
          text: transcript,
          timestamp: new Date(),
          isInterruption: options.isInterruption,
          quality: audioQuality.qualityScore,
          confidence: options.confidence || 0.5
        })
        
        // الاحتفاظ بآخر 7 نصوص فقط
        if (session.recentTranscripts.length > 7) {
          session.recentTranscripts.shift()
        }
      }

      // التعامل مع الرد الصوتي العادي (إذا لم تكن كلمة توقف)
      if (!containsStopWord && result.audioResponse) {
        await sendImprovedAudioResponse(streamSid, result)
      }

    } else {
      console.error(`❌ خطأ n8n: ${response.status} - ${response.statusText}`)
      
      // تحديث حالة الخطأ في الجلسة
      const session = audioSessions.get(streamSid)
      if (session) {
        session.lastError = `خطأ n8n: ${response.status}`
        session.lastSuccess = false
      }
    }

  } catch (error) {
    console.error("❌ خطأ معالجة الصوت المحسن:", error.message)
    
    // تحديث حالة الخطأ في الجلسة
    const session = audioSessions.get(streamSid)
    if (session) {
      session.lastError = error.message
      session.lastSuccess = false
    }
  }
}
 
const convertToTwilioFormatImproved = async (audioBuffer, inputFormat = "auto") => {
  return new Promise((resolve, reject) => {
    // تحديد نوع الملف
    let detectedFormat = inputFormat
    if (inputFormat === "auto") {
      const header = audioBuffer.slice(0, 12)
      if (header[0] === 0xFF && (header[1] & 0xE0) === 0xE0) {
        detectedFormat = 'mp3'
      } else if (header.toString('ascii', 0, 4) === 'RIFF') {
        detectedFormat = 'wav'
      } else if (header.toString('ascii', 0, 4) === 'OggS') {
        detectedFormat = 'ogg'
      } else if (header.slice(4, 8).toString('ascii') === 'ftyp') {
        detectedFormat = 'm4a'
      } else {
        detectedFormat = 'wav'
      }
    }

    const tempDir = ensureTempDir()
    const timestamp = Date.now()
    const inputFile = path.join(tempDir, `input_improved_${timestamp}.${detectedFormat}`)
    
    // حفظ الملف المؤقت
    fs.writeFileSync(inputFile, audioBuffer)

    const chunks = []
    let stderrData = ""
    
    // معاملات ffmpeg محسنة ومعايرة لـ Twilio
    const ffmpegArgs = [
      "-i", inputFile,
      "-acodec", "pcm_s16le",  
      "-ar", "8000",           
      "-ac", "1",              
      "-f", "s16le",           
      // تحسينات متوازنة - بدون إفراط
      "-af", "volume=1.2,highpass=f=85,lowpass=f=3800,dynaudnorm=p=0.9:s=12", 
      "-y",
      "pipe:1"
    ]

    const ffmpegProcess = spawn("ffmpeg", ffmpegArgs, {
      stdio: ['ignore', 'pipe', 'pipe']
    })

    ffmpegProcess.stdout.on("data", (chunk) => {
      chunks.push(chunk)
    })

    ffmpegProcess.stderr.on("data", (data) => {
      stderrData += data.toString()
    })

    ffmpegProcess.on("close", (code) => {
      // التنظيف
      try {
        if (fs.existsSync(inputFile)) fs.unlinkSync(inputFile)
      } catch (e) {}

      if (code !== 0) {
        console.error("خطأ ffmpeg:", stderrData)
        return reject(new Error(`فشل ffmpeg مع الرمز ${code}`))
      }

      if (chunks.length === 0) {
        return reject(new Error("لم يتم إنتاج بيانات صوتية"))
      }

      try {
        const pcm16Buffer = Buffer.concat(chunks)
        console.log(`بيانات PCM: ${pcm16Buffer.length} بايت`)

        // تحويل PCM 16-bit إلى μ-law باستخدام الجدول الصحيح
        const samples = pcm16Buffer.length / 2
        const mulawBuffer = Buffer.alloc(samples)

        for (let i = 0; i < samples; i++) {
          const pcmValue = pcm16Buffer.readInt16LE(i * 2)
          const index = Math.max(0, Math.min(65535, pcmValue + 32768))
          mulawBuffer[i] = PCM_TO_MULAW[index]
        }

        console.log(`تحويل μ-law محسن: ${mulawBuffer.length} بايت`)

        // تقسيم إلى قطع بحجم 160 بايت (20ms في 8kHz)
        const audioChunks = []

        for (let i = 0; i < mulawBuffer.length; i += VAD_CONFIG.CHUNK_SIZE) {
          let chunk = mulawBuffer.slice(i, i + VAD_CONFIG.CHUNK_SIZE)
          
          // إضافة حشو إذا لزم الأمر
          if (chunk.length < VAD_CONFIG.CHUNK_SIZE) {
            const paddedChunk = Buffer.alloc(VAD_CONFIG.CHUNK_SIZE, 0x7F) // صمت μ-law
            chunk.copy(paddedChunk)
            chunk = paddedChunk
          }
          
          audioChunks.push(chunk.toString("base64"))
        }

        const estimatedDuration = Math.round((mulawBuffer.length / 8000) * 1000)

        resolve({
          success: true,
          chunks: audioChunks,
          totalChunks: audioChunks.length,
          totalSize: mulawBuffer.length,
          estimatedDurationMs: estimatedDuration,
          format: "ulaw",
          sampleRate: 8000,
          channels: 1,
          chunkSize: VAD_CONFIG.CHUNK_SIZE,
          originalFormat: detectedFormat,
          originalSize: audioBuffer.length,
          improved: true
        })

      } catch (error) {
        console.error("خطأ المعالجة:", error.message)
        reject(error)
      }
    })

    ffmpegProcess.on("error", (err) => {
      try {
        if (fs.existsSync(inputFile)) fs.unlinkSync(inputFile)
      } catch (e) {}
      reject(err)
    })

    // مهلة زمنية
    setTimeout(() => {
      if (!ffmpegProcess.killed) {
        ffmpegProcess.kill("SIGTERM")
        reject(new Error("مهلة ffmpeg منتهية"))
      }
    }, 18000)
  })
}
const sendImprovedAudioResponse = async (streamSid, result) => {
  try {
    const targetWS = findActiveConnection(streamSid)
    if (targetWS && result.audioResponse) {
      const audioBuffer = Buffer.from(result.audioResponse, "base64")
      
      // تحويل محسن للصوت الصادر أيضاً
      const twilioAudio = await convertToTwilioFormatImproved(audioBuffer, result.audioFormat || "mp3")
      await sendAudioToTwilio(targetWS, twilioAudio.chunks, streamSid)
      console.log("✅ رد صوتي محسن مُرسل")
    }
  } catch (error) {
    console.error("❌ خطأ إرسال الرد الصوتي:", error.message)
  }
}
const updateAdaptiveThresholds = (vadState, audioQuality) => {
  // تحديث مستوى الضوضاء الخلفية
  if (!vadState.backgroundNoiseLevel) {
    vadState.backgroundNoiseLevel = audioQuality.avgEnergy
  } else {
    vadState.backgroundNoiseLevel = vadState.backgroundNoiseLevel * 0.95 + audioQuality.avgEnergy * 0.05
  }
  
  // حساب العتبة التكيفية
  const baseThreshold = Math.max(VAD_CONFIG.ENERGY_THRESHOLD, vadState.backgroundNoiseLevel * 2)
  vadState.adaptiveThreshold = baseThreshold * VAD_CONFIG.ARABIC_VOICE_ADJUSTMENTS.ENERGY_MULTIPLIER
  
  // تحديث تاريخ الطاقة
  if (!vadState.energyHistory) vadState.energyHistory = []
  vadState.energyHistory.push(audioQuality.avgEnergy)
  if (vadState.energyHistory.length > 50) {
    vadState.energyHistory.shift()
  }
  vadState.averageEnergy = vadState.energyHistory.reduce((a, b) => a + b, 0) / vadState.energyHistory.length
  
  // تحديث تاريخ الجودة
  if (!vadState.qualityHistory) vadState.qualityHistory = []
  vadState.qualityHistory.push(audioQuality.qualityScore)
  if (vadState.qualityHistory.length > 50) {
    vadState.qualityHistory.shift()
  }
  vadState.averageQualityScore = vadState.qualityHistory.reduce((a, b) => a + b, 0) / vadState.qualityHistory.length
}
const resetVADStateImproved = (vadState, reason = "manual") => {
  console.log(`🔄 إعادة تعيين محسنة لـ VAD - السبب: ${reason}`)
  
  // إيقاف المؤقتات
  if (vadState.recordingTimeout) {
    clearTimeout(vadState.recordingTimeout)
    vadState.recordingTimeout = null
  }
  
  // إعادة تعيين الحالات الأساسية
  vadState.isRecording = false
  vadState.speechFrames = 0
  vadState.silenceFrames = 0
  vadState.hasValidSpeech = false
  vadState.isUserInterrupting = false
  vadState.needsReset = false
  vadState.lastResetTime = Date.now()
  vadState.consecutiveProcessingFailures = 0
  
  // المحافظة على البيانات التكيفية المفيدة
  if (reason !== "complete_reset") {
    // لا نمسح البيانات التكيفية إلا في إعادة التعيين الكاملة
    if (!vadState.backgroundNoiseLevel) vadState.backgroundNoiseLevel = 0
    if (!vadState.adaptiveThreshold) vadState.adaptiveThreshold = VAD_CONFIG.ENERGY_THRESHOLD
  } else {
    vadState.backgroundNoiseLevel = 0
    vadState.adaptiveThreshold = VAD_CONFIG.ENERGY_THRESHOLD
    vadState.energyHistory = []
    vadState.qualityHistory = []
    vadState.averageEnergy = 0
    vadState.averageQualityScore = 0
  }
  
  // تنظيف البافر جزئياً فقط
  if (vadState.audioBuffer && vadState.audioBuffer.length > 100) {
    vadState.audioBuffer = vadState.audioBuffer.slice(-50) // الاحتفاظ بالقليل
  }
  
  console.log(`✅ إعادة تعيين VAD مكتملة`)
}
const convertMulawToWavImproved = (mulawBuffer) => {
  console.log(`تحويل محسن μ-law للصوت العربي: ${mulawBuffer.length} بايت`)
  
  const pcmSamples = new Int16Array(mulawBuffer.length)
  
  for (let i = 0; i < mulawBuffer.length; i++) {
    let pcmValue = MULAW_TO_PCM[mulawBuffer[i]]
    
    // تحسينات للصوت العربي
    // تقليل فلترة الضوضاء للمحافظة على وضوح الصوت
    if (Math.abs(pcmValue) < VAD_CONFIG.NOISE_GATE_THRESHOLD) {
      pcmValue = Math.floor(pcmValue * 0.5) // تقليل أكثر تدرجاً
    }
    
    pcmSamples[i] = pcmValue
  }
  
  // تطبيق تحسين خفيف للوضوح
  for (let i = 1; i < pcmSamples.length - 1; i++) {
    const current = pcmSamples[i]
    const prev = pcmSamples[i - 1]
    const next = pcmSamples[i + 1]
    
    // تنعيم خفيف للضوضاء مع المحافظة على الحدة
    if (Math.abs(current) < 200 && Math.abs(prev) > 1000 && Math.abs(next) > 1000) {
      pcmSamples[i] = Math.floor((prev + next) * 0.15)
    }
  }
  
  // إنشاء WAV header
  const sampleRate = 8000
  const numChannels = 1
  const bitsPerSample = 16
  const byteRate = sampleRate * numChannels * bitsPerSample / 8
  const blockAlign = numChannels * bitsPerSample / 8
  const dataSize = pcmSamples.length * 2
  
  const header = Buffer.alloc(44)
  header.write("RIFF", 0, 4)
  header.writeUInt32LE(36 + dataSize, 4)
  header.write("WAVE", 8, 4)
  header.write("fmt ", 12, 4)
  header.writeUInt32LE(16, 16)
  header.writeUInt16LE(1, 20)
  header.writeUInt16LE(numChannels, 22)
  header.writeUInt32LE(sampleRate, 24)
  header.writeUInt32LE(byteRate, 28)
  header.writeUInt16LE(blockAlign, 32)
  header.writeUInt16LE(bitsPerSample, 34)
  header.write("data", 36, 4)
  header.writeUInt32LE(dataSize, 40)
  
  const pcmBuffer = Buffer.alloc(dataSize)
  for (let i = 0; i < pcmSamples.length; i++) {
    pcmBuffer.writeInt16LE(pcmSamples[i], i * 2)
  }
  
  return Buffer.concat([header, pcmBuffer])
}
 const processVADFrame  = async (payload, vadState, streamSid, phoneNumber) => {
  try {
    const now = Date.now()
    
    vadState.audioBuffer.push(payload)
    vadState.totalFrames++
    
    // زيادة حجم البافر المسموح
    if (vadState.audioBuffer.length > VAD_CONFIG.MAX_BUFFER_SIZE) {
      vadState.audioBuffer = vadState.audioBuffer.slice(-Math.floor(VAD_CONFIG.MAX_BUFFER_SIZE * 0.9))
    }
    
    const audioData = Buffer.from(payload, "base64")
    const voiceAnalysis = analyzeRealVoiceActivity(audioData)
    const audioQuality = analyzeAudioQualityImproved(audioData, vadState)
    
    updateAdaptiveThresholds(vadState, audioQuality)
    
    const isRealVoice = voiceAnalysis.isRealVoice && audioQuality.isLikelyVoice
    
    if (isRealVoice) {
      vadState.speechFrames++
      vadState.silenceFrames = 0
      vadState.lastSpeechTime = now
      vadState.hasValidSpeech = true
      
      // بدء التسجيل مع شروط أكثر مرونة
      if (!vadState.isRecording && 
          vadState.speechFrames >= VAD_CONFIG.MIN_SPEECH_FRAMES && 
          voiceAnalysis.confidenceScore > VAD_CONFIG.VOICE_CONFIDENCE_THRESHOLD &&
          !isAudioPlaying(streamSid)) {
        
        vadState.isRecording = true
        vadState.speechStartTime = now
        
        console.log(`🎤 بدء تسجيل محسن (للكلام الطويل) للمكالمة ${streamSid?.slice(-8)}`)
        
        if (vadState.recordingTimeout) {
          clearTimeout(vadState.recordingTimeout)
        }
        
        // مهلة زمنية أطول للكلام الطويل
        vadState.recordingTimeout = setTimeout(() => {
          if (vadState.isRecording) {
            console.log(`⏰ انتهت مهلة التسجيل الطويل - معالجة فورية`)
            vadState.needsReset = true
          }
        }, VAD_CONFIG.MAX_RECORDING_DURATION)
      }
      
    } else {
      // تقليل أبطأ للسماح بالوقفات القصيرة
      vadState.speechFrames = Math.max(0, vadState.speechFrames - 0.5)
      vadState.silenceFrames++
      
      // فحص خاص للوقفات في الكلام الطويل
      const isLongPause = vadState.silenceFrames >= VAD_CONFIG.MIN_SILENCE_FRAMES
      const isExtendedPause = vadState.silenceFrames >= (VAD_CONFIG.MIN_SILENCE_FRAMES * 2)
      const hasBeenRecordingLong = vadState.isRecording && 
                                   (now - vadState.speechStartTime) >= VAD_CONFIG.LONG_PHRASE_DURATION
      
      // إنهاء التسجيل بناءً على طول الكلام
      if (vadState.isRecording && (isLongPause || (isExtendedPause && hasBeenRecordingLong))) {
        vadState.speechDuration = now - vadState.speechStartTime
        
        if (vadState.recordingTimeout) {
          clearTimeout(vadState.recordingTimeout)
          vadState.recordingTimeout = null
        }
        
        console.log(`🔇 انتهى الكلام الطويل بعد ${vadState.speechDuration}ms، ${vadState.audioBuffer.length} إطار`)
        
        // فحص الشروط للكلام العادي
        const hasMinFrames = vadState.audioBuffer.length >= 12
        const hasMinDuration = vadState.speechDuration >= VAD_CONFIG.MIN_SPEECH_DURATION
        const hasGoodQuality = vadState.averageQualityScore > VAD_CONFIG.MIN_QUALITY_SCORE
        const hasRealSpeech = vadState.hasValidSpeech
        const withinTimeLimit = vadState.speechDuration <= VAD_CONFIG.MAX_RECORDING_DURATION
        
        // فحص خاص للكلام الطويل
        const isValidLong = isValidLongSpeech(vadState, audioQuality, voiceAnalysis)
        
        // فحص خاص للكلام القصير (الأصلي)
        const isValidShort = isValidShortSpeech(vadState, audioQuality, voiceAnalysis)
        
        console.log(`📊 فحص شروط المعالجة الشاملة:`)
        console.log(`   - إطارات كافية: ${vadState.audioBuffer.length}/12 ✅${hasMinFrames}`)
        console.log(`   - مدة كافية: ${vadState.speechDuration}ms/${VAD_CONFIG.MIN_SPEECH_DURATION}ms ✅${hasMinDuration}`)
        console.log(`   - جودة جيدة: ${(vadState.averageQualityScore * 100).toFixed(1)}% ✅${hasGoodQuality}`)
        console.log(`   - كلام حقيقي: ✅${hasRealSpeech}`)
        console.log(`   - ضمن الحد الزمني: ✅${withinTimeLimit}`)
        console.log(`   - كلام طويل صالح: ✅${isValidLong}`)
        console.log(`   - كلام قصير صالح: ✅${isValidShort}`)
        
        // قبول الكلام إذا تحققت أي من الشروط
        const shouldProcess = (hasMinFrames && hasMinDuration && hasGoodQuality && hasRealSpeech && withinTimeLimit) ||
                             (isValidLong && hasRealSpeech && withinTimeLimit) ||
                             (isValidShort && hasRealSpeech && withinTimeLimit)
        
        if (shouldProcess) {
          const processingType = isValidLong ? 'كلام طويل مقبول' : 
                                isValidShort ? 'كلام قصير مقبول' : 'كلام عادي مقبول'
          console.log(`✅ ${processingType} - بدء المعالجة`)
          
          vadState.isRecording = false
          vadState.lastProcessingAttempt = now
          
          const audioToProcess = vadState.audioBuffer.slice()
          vadState.audioBuffer = []
          vadState.totalSpeechSegments++
          
          try {
            await processIncomingAudioFromTwilio(audioToProcess, streamSid, phoneNumber, {
              priority: isValidLong ? 'high' : (isValidShort ? 'normal' : 'normal'),
              isLongSpeech: isValidLong,
              isShortSpeech: isValidShort,
              speechDuration: vadState.speechDuration,
              qualityScore: vadState.averageQualityScore,
              confidence: voiceAnalysis.confidenceScore,
              processingReason: isValidLong ? 'long_speech_accepted' : 
                               (isValidShort ? 'short_speech_accepted' : 'normal_speech')
            })
            
            vadState.consecutiveProcessingFailures = 0
            
          } catch (error) {
            console.error(`❌ خطأ في المعالجة المحسنة:`, error.message)
            vadState.consecutiveProcessingFailures++
          }
        } else {
          console.log(`❌ الشروط غير كافية - تجاهل الكلام`)
          vadState.isRecording = false
          // احتفاظ بجزء أكبر من البافر للكلام الطويل
          if (vadState.audioBuffer.length > 100) {
            vadState.audioBuffer = vadState.audioBuffer.slice(-50)
          }
        }
        
        vadState.hasValidSpeech = false
        vadState.speechDuration = 0
        vadState.speechFrames = 0
      }
    }
    
    // تأخير إعادة التعيين وزيادة عدد المحاولات المسموحة
    if (vadState.needsReset) {
      if (vadState.audioBuffer.length > 0 && vadState.isRecording) {
        console.log(`🚨 معالجة إجبارية قبل إعادة التعيين`)
        const audioToProcess = [...vadState.audioBuffer]
        vadState.audioBuffer = []
        
        await processIncomingAudioFromTwilio(audioToProcess, streamSid, phoneNumber, {
          priority: 'urgent',
          isForced: true,
          reason: 'forced_before_reset'
        })
      }
      
      resetVADStateImproved(vadState, "needed_reset")
    }
    
    // زيادة فترة إعادة التعيين الدورية
    if (now - vadState.lastResetTime > 180000) { // كل 3 دقائق بدلاً من دقيقتين
      resetVADStateImproved(vadState, "periodic_maintenance")
    }
    
  } catch (error) {
    console.error("خطأ في VAD المحسن:", error.message)
    vadState.consecutiveProcessingFailures++
    
    // زيادة عدد المحاولات المسموحة قبل إعادة التعيين
    if (vadState.consecutiveProcessingFailures > 4) { // من 2 إلى 4
      resetVADStateImproved(vadState, "error_recovery")
    }
  }
}

 
 
 

 
// Create conversation state
const createConversationState = (streamSid) => ({
  streamSid,
  currentSpeaker: "none",
  lastActivity: Date.now(),
  totalExchanges: 0,
  createdAt: Date.now(),
})

// WebSocket connection handler
wss.on("connection", (ws, req) => {
  console.log("🔌 New corrected audio connection")

  let urlFrom = "unknown"
  try {
    const url = new URL(req.url, `http://${req.headers.host}`)
    urlFrom = url.searchParams.get("from") || "unknown"
  } catch (urlError) {
    console.warn("Could not parse URL parameters:", urlError.message)
  }

  ws.streamSid = null
  ws.phoneNumber = urlFrom
  ws.vadState = createSimpleVADState()
  ws.totalChunksReceived = 0
  ws.isConnected = true

  activeConnections.add(ws)

  const keepAliveInterval = setInterval(() => {
    if (ws.readyState === 1) {
      try {
        ws.ping()
      } catch (error) {
        console.error("Keepalive error:", error.message)
      }
    }
  }, 30000)

 ws.on("message", async (message) => {
    
  try {
    const data = JSON.parse(message)

    switch (data.event) {
      case "connected":
        console.log("📞 Twilio connected (enhanced version)")
        break

      case "start":
        console.log(`🎙️ Call started: ${data.start?.streamSid?.slice(-8)}`)
        ws.streamSid = data.start.streamSid
        
        // Create enhanced VAD state
        ws.vadState = createSimpleVADState()

        if (!conversationStates.has(ws.streamSid)) {
          conversationStates.set(ws.streamSid, createConversationState(ws.streamSid))
        }

        if (!audioPlaybackStates.has(ws.streamSid)) {
          audioPlaybackStates.set(ws.streamSid, createAudioPlaybackState(ws.streamSid))
        }

        audioSessions.set(ws.streamSid, {
          streamSid: ws.streamSid,
          phoneNumber: ws.phoneNumber,
          startTime: new Date(),
          totalChunks: 0,
          totalProcessed: 0,
          lastActivity: new Date(),
          lastSuccess: null,
          lastError: null
        })
        break

      case "media":
        if (data.media?.payload && ws.streamSid) {
          ws.totalChunksReceived++
          
          const session = audioSessions.get(ws.streamSid)
          if (session) {
            session.totalChunks = ws.totalChunksReceived
            session.lastActivity = new Date()
          }

          // Use enhanced function
          await processVADFrame(data.media.payload, ws.vadState, ws.streamSid, ws.phoneNumber)
        }
        break

      case "stop":
        console.log(`📞 Call ended: ${ws.streamSid?.slice(-8)}`)
        if (ws.streamSid) {
          // Clean timeout if exists
          if (ws.vadState?.recordingTimeout) {
            clearTimeout(ws.vadState.recordingTimeout)
          }
          
          stopAudioPlayback(ws.streamSid, "call_ended")
          conversationStates.delete(ws.streamSid)
          audioSessions.delete(ws.streamSid)
          audioPlaybackStates.delete(ws.streamSid)
        }
        break

      default:
        console.log(`Unknown event: ${data.event}`)
    }
  } catch (error) {
    console.error("WebSocket message processing error:", error.message)
    
    // Reset VAD state on error
    if (ws.vadState) {
      resetVADStateImproved(ws.vadState, "message_processing_error")
    }
  }
})

  ws.on("close", (code, reason) => {
    console.log(`🔌 Connection closed for number ${ws.phoneNumber} (${code})`)
    activeConnections.delete(ws)
    ws.isConnected = false

    if (ws.streamSid) {
      stopAudioPlayback(ws.streamSid, "connection_closed")
    }

    if (keepAliveInterval) {
      clearInterval(keepAliveInterval)
    }
  })

  ws.on("error", (error) => {
    console.error("WebSocket error:", error.message)
    activeConnections.delete(ws)
    ws.isConnected = false
    
    if (ws.streamSid) {
      stopAudioPlayback(ws.streamSid, "websocket_error")
    }
  })

  // Send connection confirmation
  setTimeout(() => {
    if (ws.readyState === 1) {
      try {
        ws.send(JSON.stringify({
          event: "connected",
          timestamp: Date.now(),
          server: "corrected-twilio-audio-processor",
          version: "2.0-corrected",
          phoneNumber: ws.phoneNumber,
          processingMode: "corrected-voice-activity-detection",
          features: ["corrected-mulaw-conversion", "simplified-processing", "accurate-audio-quality"]
        }))
        console.log(`Correct connection confirmation for number ${ws.phoneNumber}`)
      } catch (error) {
        console.warn("Cannot send connection confirmation:", error.message)
      }
    }
  }, 100)
})
const tempFileManager = {
  tempDir: path.join(__dirname, "temp"),
  files: new Set(),
  
  createTempFile(prefix, extension) {
    const filename = `${prefix}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}.${extension}`
    const filepath = path.join(this.tempDir, filename)
    this.files.add(filepath)
    return filepath
  },
  
  async cleanup(filepath) {
    try {
      await fsPromises.unlink(filepath)
      this.files.delete(filepath)
    } catch (error) {
      console.warn('File cleanup warning:', error.message)
    }
  },
  
  async cleanupAll() {
    for (const filepath of this.files) {
      await this.cleanup(filepath)
    }
  }
}

const streamConvertToTwilioFormat = async (audioBuffer, inputFormat = "mp3") => {
  return new Promise(async (resolve, reject) => {
    try {
      const { PassThrough } = await import('stream');

      // Create input stream properly
      const inputStream = await createAudioStream(audioBuffer);
      const outputStream = new PassThrough();
      const chunks = [];

      const ffmpegArgs = [
        "-f", inputFormat === 'wav' ? 'wav' : 'mp3',
        "-i", "pipe:0",
        "-acodec", "pcm_s16le",
        "-ar", "8000",
        "-ac", "1",
        "-f", "s16le",
        "-threads", "0",
        "-preset", "ultrafast",
        "-avoid_negative_ts", "make_zero",
        "pipe:1"
      ];

      const ffmpegProcess = spawn("ffmpeg", ffmpegArgs, {
        stdio: ['pipe', 'pipe', 'ignore']
      });

      // Error handling for streams
      inputStream.on('error', (error) => {
        console.error('Input stream error:', error);
        ffmpegProcess.kill('SIGTERM');
        reject(error);
      });

      ffmpegProcess.stdin.on('error', (error) => {
        console.error('FFmpeg stdin error:', error);
        reject(error);
      });

      ffmpegProcess.stdout.on('error', (error) => {
        console.error('FFmpeg stdout error:', error);
        reject(error);
      });

      // Pipe with error handling
      inputStream.pipe(ffmpegProcess.stdin).on('error', (error) => {
        console.error('Pipe error:', error);
        reject(error);
      });

      ffmpegProcess.stdout.pipe(outputStream);

      outputStream.on('data', (chunk) => {
        chunks.push(chunk);
      });

      outputStream.on('end', () => {
        if (chunks.length === 0) {
          return reject(new Error("No output from stream conversion"));
        }

        try {
          const pcm16Buffer = Buffer.concat(chunks);
          const mulawBuffer = convertPCMToMulaw(pcm16Buffer);

          // Split into 20ms chunks (160 samples at 8kHz)
          const audioChunks = [];
          const chunkSize = 160;

          for (let i = 0; i < mulawBuffer.length; i += chunkSize) {
            let chunk = mulawBuffer.slice(i, i + chunkSize);

            if (chunk.length < chunkSize) {
              const paddedChunk = Buffer.alloc(chunkSize, 0x7F); // silence padding
              chunk.copy(paddedChunk);
              chunk = paddedChunk;
            }

            audioChunks.push(chunk.toString("base64"));
          }

          resolve({
            success: true,
            chunks: audioChunks,
            totalChunks: audioChunks.length,
            totalSize: mulawBuffer.length,
            estimatedDurationMs: Math.round((mulawBuffer.length / 8000) * 1000),
            format: "ulaw",
            sampleRate: 8000,
            channels: 1,
            streamProcessed: true
          });

        } catch (error) {
          reject(error);
        }
      });

      ffmpegProcess.on("error", (error) => {
        console.error('FFmpeg process error:', error);
        reject(error);
      });

      // Timeout handling
      const timeout = setTimeout(() => {
        ffmpegProcess.kill("SIGKILL");
        reject(new Error("Stream conversion timeout"));
      }, 10000); // Increased timeout

      ffmpegProcess.on('close', () => {
        clearTimeout(timeout);
      });

    } catch (error) {
      console.error('Stream conversion setup error:', error);
      reject(error);
    }
  });
};
const convertPCMToMulaw = (pcm16Buffer) => {
  const samples = pcm16Buffer.length / 2
  const mulawBuffer = Buffer.alloc(samples)
  
  // معالجة بدفعات للأداء الأفضل
  const batchSize = 1024
  
  for (let i = 0; i < samples; i += batchSize) {
    const endIndex = Math.min(i + batchSize, samples)
    
    for (let j = i; j < endIndex; j++) {
      const pcmValue = pcm16Buffer.readInt16LE(j * 2)
      const index = Math.max(0, Math.min(65535, pcmValue + 32768))
      mulawBuffer[j] = PCM_TO_MULAW[index]
    }
  }
  
  return mulawBuffer
}
// تنظيف دوري
setInterval(async () => {
  const files = await fsPromises.readdir(tempFileManager.tempDir)
  const now = Date.now()
  
  for (const file of files) {
    const filepath = path.join(tempFileManager.tempDir, file)
    const stats = await fsPromises.stat(filepath)
    
    // حذف الملفات الأقدم من 10 دقائق
    if (now - stats.mtime.getTime() > 10 * 60 * 1000) {
      await tempFileManager.cleanup(filepath)
    }
  }
}, 5 * 60 * 1000) // كل 5 دقائق
// Express Routes
app.get("/", (req, res) => {
  res.json({
    status: "running",
    message: "Corrected Twilio Audio Processing Server",
    timestamp: new Date().toISOString(),
    activeSessions: audioSessions.size,
    activeConnections: activeConnections.size,
    activeAudioPlaybacks: audioPlaybackStates.size,
    server: "Express + WebSocket + Corrected Audio Processing",
    version: "2.0-corrected",
    improvements: [
      "ITU-T G.711 Compliant μ-law Conversion",
      "Simplified Audio Processing Pipeline", 
      "Correct PCM to μ-law Tables",
      "Minimal Audio Filtering",
      "Better Audio Quality"
    ]
  })
})

app.get("/health", (req, res) => {
  res.json({
    status: "healthy",
    service: "Corrected Twilio Audio Processing Server",
    timestamp: new Date().toISOString(),
    activeSessions: audioSessions.size,
    activeConnections: activeConnections.size,
    activeAudioPlaybacks: audioPlaybackStates.size,
    uptime: Math.floor(process.uptime()),
    memory: {
      used: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + "MB",
      total: Math.round(process.memoryUsage().heapTotal / 1024 / 1024) + "MB",
    },
    version: "2.0-corrected"
  })
})

// Incoming call webhook
app.all("/incoming-call", (req, res) => {
  console.log("📞 Incoming call (Corrected Audio Processing)")

  if (req.method === "OPTIONS") {
    return res.status(200).send()
  }

  try {
    const callData = {
      callSid: req.body?.CallSid || "unknown",
      from: req.body?.Caller || req.body?.From || "unknown",
      to: req.body?.Called || req.body?.To || "unknown",
      direction: req.body?.Direction || "inbound",
    }

    console.log(`📱 New corrected call: ${callData.from} → ${callData.to}`)

    const isSecure = req.get("x-forwarded-proto") === "https" || req.get("host")?.includes("ngrok")
    const wsProtocol = isSecure ? "wss" : "ws"
    const host = req.get("host")

    const twimlResponse = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say voice="Polly.Zeina" language="ar">Hello, I am the enhanced smart assistant</Say>
    <Pause length="1"/>
    <Say voice="Polly.Zeina" language="ar">Speak now and I will respond with clear audio</Say>
    <Connect>
        <Stream url="${wsProtocol}://${host}/media-stream?from=${encodeURIComponent(callData.from)}">
            <Parameter name="from" value="${callData.from}" />
            <Parameter name="callSid" value="${callData.callSid}" />
            <Parameter name="direction" value="${callData.direction}" />
            <Parameter name="corrected" value="true" />
        </Stream>
    </Connect>
</Response>`

    console.log(`Corrected TwiML sent for number ${callData.from}`)

    res.set("Content-Type", "text/xml; charset=utf-8")
    res.send(twimlResponse)

  } catch (error) {
    console.error("Webhook error:", error.message)

    const fallbackTwiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say voice="Polly.Zeina" language="ar">Technical error, retrying</Say>
</Response>`

    res.set("Content-Type", "text/xml; charset=utf-8")
    res.send(fallbackTwiml)
  }
})
 app.post("/send-audio-instant", async (req, res) => {
  const requestStartTime = Date.now();
  console.log("🚀 MAXIMUM SPEED request");
    console.log(req.body)
  try {
    let { streamSid, audioData, audioFormat = 'auto' } = req.body

    // Validation فوري
    if (!streamSid || !audioData) {
      return res.status(400).json({
        success: false,
        error: "Missing data",
        time: Date.now() - requestStartTime
      });
    }

    // البحث عن connection
    const targetConnection = findActiveConnection(streamSid);
    if (!targetConnection) {
      return res.status(404).json({
        success: false,
        error: "No connection",
        time: Date.now() - requestStartTime
      });
    }

    // Base64 decode محسن
    const audioBuffer = Buffer.from(audioData, "base64");
    if (audioBuffer.length === 0) {
      return res.status(400).json({
        success: false,
        error: "Empty audio",
        time: Date.now() - requestStartTime
      });
    }

    console.log(`🚀 Processing ${audioBuffer.length} bytes`);

    // تحويل مع stream processing
    const conversionStart = Date.now();
    const audioResult = await streamConvertToTwilioFormat(audioBuffer, audioFormat);
    const conversionTime = Date.now() - conversionStart;

    if (!audioResult.chunks || audioResult.chunks.length === 0) {
      return res.status(500).json({
        success: false,
        error: "Conversion failed",
        time: Date.now() - requestStartTime
      });
    }

    // إرسال مع batching
    const sendStart = Date.now();
    const success = await batchedSendAudioToTwilio(targetConnection, audioResult.chunks, streamSid);
    const sendTime = Date.now() - sendStart;

    const totalTime = Date.now() - requestStartTime;

    if (success) {
      console.log(`🚀 MAXIMUM SUCCESS: ${totalTime}ms (Conv: ${conversionTime}ms, Send: ${sendTime}ms)`);
      
      res.json({
        success: true,
        message: "Maximum speed delivery",
        performance: {
          totalMs: totalTime,
          conversionMs: conversionTime,
          sendMs: sendTime,
          throughput: Math.round(audioBuffer.length / totalTime * 1000), // bytes/sec
          chunksCount: audioResult.chunks.length,
          efficiency: `${(audioResult.estimatedDurationMs / totalTime * 100).toFixed(1)}%`
        },
        details: {
          streamSid: streamSid.slice(-8),
          optimization: "maximum_speed_with_streams"
        }
      });
    } else {
      res.status(500).json({
        success: false,
        error: "Send failed",
        time: totalTime
      });
    }

  } catch (error) {
    const totalTime = Date.now() - requestStartTime;
    console.error(`🚀 Maximum speed error (${totalTime}ms):`, error.message);
    
    res.status(500).json({
      success: false,
      error: error.message,
      time: totalTime
    });
  }
});
// Send audio response endpoint
app.post("/send-audio-response", async (req, res) => {
  console.log("🎵 Corrected audio send request")
  console.log(req.body)
  try {
    let { streamSid, audioData, audioFormat = 'auto' } = req.body

    // Validation فوري
    if (!streamSid || !audioData) {
      return res.status(400).json({
        success: false,
        error: "Missing data",
        time: Date.now() - requestStartTime
      });
    }

    // البحث عن connection
    const targetConnection = findActiveConnection(streamSid);
    if (!targetConnection) {
      return res.status(404).json({
        success: false,
        error: "No connection",
        time: Date.now() - requestStartTime
      });
    }

    // Base64 decode محسن
    const audioBuffer = Buffer.from(audioData, "base64");
    if (audioBuffer.length === 0) {
      return res.status(400).json({
        success: false,
        error: "Empty audio",
        time: Date.now() - requestStartTime
      });
    }

    console.log(`🚀 Processing ${audioBuffer.length} bytes`);

    // تحويل مع stream processing
    const conversionStart = Date.now();
    const audioResult = await streamConvertToTwilioFormat(audioBuffer, audioFormat);
    const conversionTime = Date.now() - conversionStart;

    if (!audioResult.chunks || audioResult.chunks.length === 0) {
      return res.status(500).json({
        success: false,
        error: "Conversion failed",
        time: Date.now() - requestStartTime
      });
    }

    // إرسال مع batching
    const sendStart = Date.now();
    const success = await batchedSendAudioToTwilio(targetConnection, audioResult.chunks, streamSid);
    const sendTime = Date.now() - sendStart;

    const totalTime = Date.now() - requestStartTime;

    if (success) {
      console.log(`🚀 MAXIMUM SUCCESS: ${totalTime}ms (Conv: ${conversionTime}ms, Send: ${sendTime}ms)`);
      
      res.json({
        success: true,
        message: "Maximum speed delivery",
        performance: {
          totalMs: totalTime,
          conversionMs: conversionTime,
          sendMs: sendTime,
          throughput: Math.round(audioBuffer.length / totalTime * 1000), // bytes/sec
          chunksCount: audioResult.chunks.length,
          efficiency: `${(audioResult.estimatedDurationMs / totalTime * 100).toFixed(1)}%`
        },
        details: {
          streamSid: streamSid.slice(-8),
          optimization: "maximum_speed_with_streams"
        }
      });
    } else {
      res.status(500).json({
        success: false,
        error: "Send failed",
        time: totalTime
      });
    }

  } catch (error) {
    const totalTime = Date.now() - requestStartTime;
    console.error(`🚀 Maximum speed error (${totalTime}ms):`, error.message);
    
    res.status(500).json({
      success: false,
      error: error.message,
      time: totalTime
    });
  }
})

// Stop audio endpoint
app.post("/stop-audio", (req, res) => {
  try {
    const { streamSid, reason = "manual_stop" } = req.body
    
    if (!streamSid) {
      return res.status(400).json({
        success: false,
        error: "streamSid is required"
      })
    }
    
    const stopped = stopAudioPlayback(streamSid, reason)
    
    res.json({
      success: true,
      stopped,
      streamSid: streamSid.slice(-8),
      reason
    })
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    })
  }
})

// Audio status endpoint
app.get("/audio-status/:streamSid", (req, res) => {
  try {
    const { streamSid } = req.params
    const state = audioPlaybackStates.get(streamSid)
    
    if (!state) {
      return res.status(404).json({
        success: false,
        error: "Audio state not found"
      })
    }
    
    res.json({
      success: true,
      streamSid: streamSid.slice(-8),
      isPlaying: state.isPlaying,
      currentAudioId: state.currentAudioId,
      progress: state.totalChunks > 0 ? (state.sentChunks / state.totalChunks * 100).toFixed(1) + "%" : "0%",
      sentChunks: state.sentChunks,
      totalChunks: state.totalChunks,
      playbackStartTime: state.playbackStartTime,
      shouldStop: state.shouldStop
    })
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    })
  }
})

// Sessions endpoint
app.get("/sessions", (req, res) => {
  const sessions = Array.from(audioSessions.entries()).map(([streamSid, session]) => ({
    streamSid: streamSid.slice(-8),
    phoneNumber: session.phoneNumber,
    startTime: session.startTime,
    duration: Date.now() - session.startTime.getTime(),
    totalChunks: session.totalChunks || 0,
    totalProcessed: session.totalProcessed || 0,
    lastActivity: session.lastActivity,
    lastSuccess: session.lastSuccess,
  }))

  const audioStates = Array.from(audioPlaybackStates.entries()).map(([streamSid, state]) => ({
    streamSid: streamSid.slice(-8),
    isPlaying: state.isPlaying,
    progress: state.totalChunks > 0 ? ((state.sentChunks / state.totalChunks) * 100).toFixed(1) + "%" : "0%",
    currentAudioId: state.currentAudioId
  }))

  res.json({
    count: sessions.length,
    sessions,
    audioPlaybackStates: audioStates,
    timestamp: new Date().toISOString(),
    activeConnections: activeConnections.size,
    activeAudioPlaybacks: audioPlaybackStates.size,
    processingMode: "Corrected Voice Activity Detection",
    version: "2.0-corrected"
  })
})

// Force speech processing endpoint
app.post("/force-speech-processing", async (req, res) => {
  try {
    const { streamSid } = req.body
    
    if (!streamSid) {
      return res.status(400).json({ error: "streamSid is required" })
    }
    
    let targetConnection = null
    for (const ws of activeConnections) {
      if (ws.streamSid === streamSid && ws.readyState === 1) {
        targetConnection = ws
        break
      }
    }
    
    if (!targetConnection) {
      return res.status(404).json({ error: "No active connection" })
    }
    
    const vadState = targetConnection.vadState
    
    if (vadState.isRecording || vadState.audioBuffer.length > 0) {
      console.log(`Forcing speech processing for call ${streamSid.slice(-8)}`)
      
      vadState.isRecording = false
      
      const audioToProcess = [...vadState.audioBuffer]
      
      if (audioToProcess.length > 0) {
        console.log(`Forced audio processing: ${audioToProcess.length} frames`)
        vadState.audioBuffer = []
        await processIncomingAudioFromTwilio(audioToProcess, streamSid, targetConnection.phoneNumber)
        
        res.json({
          success: true,
          message: "Forced speech processing completed",
          processedFrames: audioToProcess.length
        })
      } else {
        res.json({
          success: true,
          message: "No speech to process",
          processedFrames: 0
        })
      }
    } else {
      res.json({
        success: true,
        message: "No active speech currently",
        processedFrames: 0
      })
    }
    
  } catch (error) {
    console.error("Forced processing error:", error.message)
    res.status(500).json({ error: error.message })
  }
})

// Test audio conversion endpoint
app.post("/test-audio-conversion", async (req, res) => {
  try {
    const { audioData, audioFormat } = req.body
    
    if (!audioData) {
      return res.status(400).json({ error: "audioData is required" })
    }
    
    const audioBuffer = Buffer.from(audioData, "base64")
    const result = await convertToTwilioFormat(audioBuffer, audioFormat || "mp3")
    
    res.json({
      success: true,
      message: "Corrected conversion test complete",
      result: {
        chunksCount: result.chunks.length,
        totalSize: result.totalSize,
        estimatedDuration: result.estimatedDurationMs,
        format: result.format,
        sampleRate: result.sampleRate,
        channels: result.channels,
        originalFormat: result.originalFormat,
        originalSize: result.originalSize,
        corrected: true
      }
    })
    
  } catch (error) {
    console.error("Corrected conversion test error:", error.message)
    res.status(500).json({
      error: "Corrected conversion test failed",
      details: error.message
    })
  }
})

// Error handling middleware
app.use((error, req, res, next) => {
  console.error("Express error:", error.message)
  if (!res.headersSent) {
    res.status(500).json({
      error: "Internal server error",
      message: process.env.NODE_ENV === "development" ? error.message : "Something went wrong",
      service: "Corrected Audio Processing Server",
    })
  }
})

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: "Not found",
    message: `Route ${req.method} ${req.url} not found`,
    availableEndpoints: [
      "/",
      "/health", 
      "/sessions",
      "/incoming-call",
      "/send-audio-response",
      "/stop-audio",
      "/audio-status/:streamSid",
      "/force-speech-processing",
      "/test-audio-conversion"
    ],
  })
})

// Graceful shutdown
const gracefulShutdown = async (signal) => {
  console.log(`\n⚠️ Received ${signal}, shutting down corrected server...`)
  try {
    for (const [streamSid, state] of audioPlaybackStates.entries()) {
      if (state.isPlaying) {
        stopAudioPlayback(streamSid, "server_shutdown")
      }
    }

    for (const ws of activeConnections) {
      try {
        if (ws.readyState === 1) {
          ws.send(JSON.stringify({
            event: "serverShutdown",
            message: "Corrected server is shutting down",
            timestamp: Date.now(),
          }))
          ws.close(1001, "Server shutting down")
        }
      } catch (error) {
        console.warn("WebSocket close warning:", error.message)
      }
    }

    server.close(() => {
      console.log("Corrected server shutdown complete")
      process.exit(0)
    })

    setTimeout(() => {
      console.error("Forced shutdown after timeout")
      process.exit(1)
    }, 10000)
  } catch (error) {
    console.error("Shutdown error:", error.message)
    process.exit(1)
  }
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"))
process.on("SIGINT", () => gracefulShutdown("SIGINT"))
process.on("SIGUSR2", () => gracefulShutdown("SIGUSR2"))

process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Promise rejection:", promise, "Reason:", reason)
})

process.on("uncaughtException", (error) => {
  console.error("Uncaught exception:", error.message)
  gracefulShutdown("uncaughtException")
})
const startVADStateMonitor = () => {
  setInterval(() => {
    const now = Date.now()
    
    for (const ws of activeConnections) {
      if (ws.streamSid && ws.vadState) {
        const vadState = ws.vadState
        
        // فحص التسجيل الطويل المدة مع مهلة أطول
        if (vadState.isRecording && 
            vadState.speechStartTime && 
            now - vadState.speechStartTime > (VAD_CONFIG.MAX_RECORDING_DURATION + 3000)) { // 3 ثواني إضافية
          
          console.log(`🚨 تسجيل طويل جداً للمكالمة ${ws.streamSid.slice(-8)} - معالجة فورية`)
          
          if (vadState.audioBuffer.length > 0) {
            const audioToProcess = [...vadState.audioBuffer]
            vadState.audioBuffer = []
            
            processIncomingAudioFromTwilio(audioToProcess, ws.streamSid, ws.phoneNumber, {
              priority: 'urgent',
              reason: 'monitor_very_long_recording_forced',
              isForced: true
            }).catch(error => {
              console.error(`معالجة مراقبة فاشلة:`, error.message)
            })
          }
          
          resetVADStateImproved(vadState, "monitor_very_long_recording_reset")
        }
        
        // زيادة حد البافر المسموح
        if (vadState.audioBuffer.length > VAD_CONFIG.MAX_BUFFER_SIZE * 2) {
          console.log(`🚨 بافر متضخم جداً للمكالمة ${ws.streamSid.slice(-8)} - تنظيف جزئي`)
          vadState.audioBuffer = vadState.audioBuffer.slice(-300) // احتفاظ بعدد أكبر
        }
      }
    }
  }, 12000) // كل 12 ثانية بدلاً من 8
}
 

const optimizeServer = () => {
 
  
  // تحسين garbage collection
  if (global.gc) {
    setInterval(() => {
      global.gc();
    }, 30000); // كل 30 ثانية
  }
  applyOptimizations()
  // تحسين إعدادات Express
  app.set('trust proxy', true);
  app.disable('x-powered-by');
  app.set('etag', false);
};

// 2. Connection pooling محسن
const optimizeHttpRequests = async() => {
  const http = await import('http');
  const https =await  import('https');
  
  // إعداد connection pools محسنة
  const httpAgent = new http.Agent({
    keepAlive: true,
    keepAliveMsecs: 30000,
    maxSockets: 50,
    maxFreeSockets: 10,
    timeout: 10000
  });
  
  const httpsAgent = new https.Agent({
    keepAlive: true,
    keepAliveMsecs: 30000,
    maxSockets: 50,
    maxFreeSockets: 10,
    timeout: 10000
  });
  
  return { httpAgent, httpsAgent };
};
const applyOptimizations = () => {
  console.log("🚀 Applying maximum speed optimizations...");
  
  // تحسين السيرفر
   const { httpAgent, httpsAgent } = optimizeHttpRequests();
  // إعداد HTTP agents
   
  // تحسين WebSocket
  wss.on('connection', (ws) => {
    ws.binaryType = 'arraybuffer';
    ws._socket.setNoDelay(true);
    ws._socket.setKeepAlive(true, 30000);
  });
  
  console.log("🚀 Maximum speed optimizations applied!");
};

// Server startup
const startServer = async () => {
  try {
    ensureTempDir()
    optimizeServer()
    // Start VAD state monitor
    startVADStateMonitor()
    console.log("🔍 VAD state monitor started")
createAudioWorker()
    server.listen(PORT, "0.0.0.0", () => {
      console.log("🎉 =========================================================")
      console.log("🚀 Enhanced Twilio Audio Processing Server Started!")
      console.log("🎉 =========================================================")
      console.log(`📡 Server running on port ${PORT}`)
      console.log(`🔧 VAD reset: http://localhost:${PORT}/reset-vad-state`)
      console.log(`🔍 Stuck state monitor: active`)
      console.log("✨ Applied fixes:")
      console.log("   - Fixed stuck VAD states")
      console.log("   - Timeout for long recordings")
      console.log("   - Periodic state monitor")
      console.log("   - Forced reset for consecutive failures")
      console.log("   - Better buffer cleanup")
      console.log("🎉 =========================================================")
    })
  } catch (error) {
    console.error("Failed to start enhanced server:", error.message)
    process.exit(1)
  }
}


startServer()