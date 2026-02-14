
#define TINY_GSM_MODEM_SIM7600
#include <TinyGsmClient.h>
#include <PubSubClient.h>
#include "FS.h"
#include "SD.h"
#include <SPI.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_BME280.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SH110X.h>
#include <Adafruit_ADS1X15.h>
#include "time.h"
#include <Wire.h>
#include <ModbusMaster.h>
#include <limits.h>
#include <math.h>

// =================== USER CONFIG ===================
#define MODEM_TX 17
#define MODEM_RX 16

// Relay pins
#define RELAY_SYS_PIN 4      // pin untuk mengontrol relay indikator sistem / inisialisasi
#define RELAY_CHARGE_PIN 26  // pin untuk mengontrol relay indikator charging (PZEM)

// RS485 control
#define EN_RS485 32

#define SerialAT Serial1
TinyGsm modem(SerialAT);
TinyGsmClient gsmClient(modem);
PubSubClient mqttClient(gsmClient);

// Modbus (PZEM-017)
ModbusMaster node;
#define SLAVE_ADDR 0x01
// Control callbacks for RS485 transceiver
void preTransmission() { digitalWrite(EN_RS485, HIGH); }
void postTransmission() { digitalWrite(EN_RS485, LOW); }

// ---------------- MQTT (MQTT BASIC) ----------------
const char* mqtt_server   = "mqtt.thingsboard.cloud";
const int   mqtt_port     = 1883;

const char* mqtt_clientId = "14ba1y7kw2p7qbbosfr2";
const char* mqttUsername  = "dnv855tqgrjga6aibudf";
const char* mqttPassword  = "dfie3pg7o18yq6q0yrug";

// Telemetry topic ThingsBoard
const char* topic_telemetry = "v1/devices/me/telemetry";

// ===== PATCH V15: MQTT reconnect guard =====
unsigned long lastMQTTAttempt = 0;
const unsigned long MQTT_RETRY_INTERVAL = 10000UL; // 10 detik

// ---------------- APN ----------------
const char apn[]  = "internet";
const char gprsUser[] = "";
const char gprsPass[] = "";

// ---------------- BME & OLED & SD ----------------
Adafruit_BME280 bme;
Adafruit_SH1106G display = Adafruit_SH1106G(128, 64, &Wire, -1);
#define SD_CS 5
// OUTBOX file for queued MQTT messages
#define OUTBOX_FILE "/outbox.csv"
// CSV daily file prefix "data_YYYYMMDD.csv"
#define DATA_FILE_PREFIX "data_"
#define DATA_FILE_SUFFIX ".csv"

// ===== PATCH V15: SD CSV write batching =====
#define CSV_BATCH_SIZE 3

struct CsvRowBuffer {
  char tanggal[16];
  char waktu[16];
  float waterPressure;
  float battV;
  float battPct;
  float temperature;
  float humidity;
  float airPressure;
  unsigned long bytesCycle;
  unsigned long long bytesTotal;
};

CsvRowBuffer csvBuf[CSV_BATCH_SIZE];
int csvBufCount = 0;
unsigned long lastCsvFlushMillis = 0;
const unsigned long CSV_FLUSH_INTERVAL_MS = 60000UL; // flush buffer minimal tiap 60 detik

// --- ADS1115: gunakan alamat 0x4B (ADDR -> SCL) sesuai konfigurasi hardwaremu
Adafruit_ADS1115 ads; // alamat I2C akan dipass ke ads.begin(address)

// --- Pembagi tegangan untuk sensor Wishner ---
const float R1 = 39000.0;
const float R2 = 100000.0;
const float dividerRatio = R2 / (R1 + R2);

// --- Sensor Wishner WPT83G scaling ---
const float SENSOR_VMIN = 0.5;
const float SENSOR_VMAX = 4.5;
const float PRESSURE_MIN = 0.0;
const float PRESSURE_MAX = 6.0;

// --- Offset Tekanan ---
float pressureOffset = 0.00;

// --- Mode Uji ---
bool manualMode = false;
float wishnerPressure = 0.0;

// NTP
const char* ntpServer = "pool.ntp.org";
const long gmtOffset_sec = 7 * 3600;
const int daylightOffset_sec = 0;

// Timing
unsigned long lastTime = 0;
unsigned long timerDelay = 300000; // 5 menit sampling (ubah ke 20000 untuk 20 detik saat testing)

// Modem monitoring
bool modemConnected = true;
unsigned long lastModemCheck = 0;
const unsigned long modemCheckInterval = 60000UL;
const int RECONNECT_TRIES = 3;

// Data
float suhu = 0.0, kelembapan = 0.0, tekanan = 0.0;

// PZEM global readings (keperluan OLED dan telemetry)
float pzemVoltage = 0.0, pzemCurrent = 0.0, pzemPower = 0.0, pzemEnergy = 0.0;

// Battery SOC (percentage)
float batterySOC = 0.0; // 0..100 %

// Velocity (predicted) global so OLED and telemetry can access it
float velocity = 0.0f;

float flow = (2.8 - velocity) *31.4;

// ===== PATCH V15: velocity clamp =====
const float VELOCITY_MIN = 0.0f;
const float VELOCITY_MAX = 10.0f; // sesuaikan kondisi pipa

unsigned long long totalDataByte = 0ULL;
const float overheadFactor = 4.0;

// Status error global
bool systemError = false;

// Retry / recovery
unsigned long lastRecoverAttempt = 0;
const unsigned long RECOVER_INTERVAL = 5 * 60 * 1000UL; // 5 minutes
int recoverAttempts = 0;
const int MAX_RECOVER_ATTEMPTS = 3;

// SD capacity handling (approximation, set to your SD size in bytes)
const uint64_t APPROX_SD_CAPACITY_BYTES = 16ULL * 1024ULL * 1024ULL * 1024ULL;// ~16GB (GiB) approx
const float SD_FULL_THRESHOLD = 0.95; // 95%

// Outbox flush batch size (to avoid blocking)
const int OUTBOX_FLUSH_BATCH = 10;

// ADS detection flag
bool adsDetected = false;

// --- PZEM settings ---
float currentOffset = -0.3; // koreksi arus PZEM
const bool PZEM_USE_HOLDING_REGISTERS = false; // jika true gunakan readHoldingRegisters (FC3), else readInputRegisters (FC4)

// Relay hysteresis / debounce (charging)
const float CHARGE_ON_TH = 0.2;   // ON jika > 0.2 A
const float CHARGE_OFF_TH = 0.1; // OFF jika < 0.1 A
const unsigned long RELAY_MIN_TOGGLE_MS = 5000; // minimal 5s antar toggles
bool relayChargeState = false;
unsigned long lastRelayChange = 0;

// ---------------- OLED cycle / scheduling ----------------
unsigned long lastOLEDCycle = 0;
const unsigned long OLED_CYCLE_INTERVAL = 500UL; // 0.5 seconds per page
int oledPage = 0;
bool oledWasOn = false;
bool oledInitialized = false;

// ======================
// Voltage-only SOC heuristic parameters & state
// ======================
const float VOLTAGE_EMA_ALPHA = 0.50f; // EMA alpha for voltage smoothing
float filteredVoltage = 0.0f;
unsigned long lastVoltageMillis = 0;

// slope threshold (V per second) to detect charge/discharge
const float SLOPE_THRESHOLD_V_PER_S = 0.0006f; // ~0.036 V/min
bool inRestVoltage = false;
unsigned long restStartMillisVoltage = 0;
const unsigned long REST_TIME_MS_V = 5 * 60 * 1000UL; // 5 minutes

enum ChargeState { CS_UNKNOWN=0, CS_CHARGING=1, CS_DISCHARGING=2, CS_REST=3 };
ChargeState chargeState = CS_UNKNOWN;

// blending factors (alpha) for updating SOC from voltage per state
const float ALPHA_REST = 0.90f;      // strong trust on voltage when at rest
const float ALPHA_CHARGING = 0.40f; // low trust while charging (surface charge)
const float ALPHA_DISCHARGING = 0.50f; // moderate trust while discharging
const float ALPHA_UNKNOWN = 0.25f;

// ---------------- Watchdog / Auto-restart configuration ----------------
// Watchdog timeout (seconds)
#define WDT_TIMEOUT_S 180            // WDT timeout in seconds (sesuaikan)
// If systemError persists longer than this, force restart (ms)
#define SYSTEM_ERROR_RESTART_MS (10UL * 60UL * 1000UL) // restart jika systemError bertahan > 10 menit

// Software watchdog tracking (replacement for esp_task_wdt API to avoid compatibility issues)
unsigned long wdtLastKick = 0;

// track when systemError started
unsigned long systemErrorSince = 0;

// ---------------- Hour cache for schedule and OLED fallback ----------------
int cachedHour = -1;
unsigned long cachedHourMillis = 0;
const unsigned long HOUR_CACHE_MS = 5 * 60 * 1000UL; // cache modem hour for 5 minutes

// ---------------- Velocity model (Nomogram SDR17 - water) ----------------
// Nomogram pada GF Piping Systems menggunakan input:
//   - di (inner diameter pipa) dalam mm
//   - Δp per meter (pressure drop) dalam mbar/m
// Output: v (m/s)
//
// Dari data digitasi nomogram SDR17 (Sheet2 di SDR17.xlsx), pendekatan yang jauh lebih cocok
// daripada model linear adalah bentuk fisik: v ~ sqrt(di * (Δp/L))
// Hasil fitting sederhana:
//   v = NOMO_A * sqrt(di_mm * dp_mbar_per_m) + NOMO_B
const float NOMO_A = 0.1032658222f;
const float NOMO_B = -0.0183057214f;

// Default di pipa (mm) — SESUAI NOMOGRAM (di / inner diameter), bukan "DN" nominal.
// Catatan: pipa yang tertulis 1/2" seringkali tidak sama dengan di=12.7 mm.
// Di data nomogram SDR17 yang kamu kirim, ukuran terkecil adalah di=21 mm.
float pipeDi_mm = 18.8f; // ID PVC AW 1/2" (JIS): OD 22 mm, tebal ~1.6 mm, ID ~18.8 mm (sesuai tabel kamu)

// Kalau kamu belum punya 2 sensor pressure (P1 & P2) untuk menghitung Δp/L,
// kamu bisa pakai asumsi kasar dari lapangan.
// Contoh kamu: Δp total ~0.1 bar sepanjang 3–3.5 m.
const float ASSUMED_DP_BAR_TOTAL = 0.1f;
const float ASSUMED_DISTANCE_M = 3.25f; // ambil tengah 3..3.5 m
const float ASSUMED_DP_MBAR_PER_M = (ASSUMED_DP_BAR_TOTAL / ASSUMED_DISTANCE_M) * 1000.0f; // 1 bar = 1000 mbar

float predictVelocityNomogram(float di_mm, float dp_mbar_per_m) {
  if (di_mm <= 0.0f || dp_mbar_per_m <= 0.0f) return 0.0f;
  return NOMO_A * sqrtf(di_mm * dp_mbar_per_m) + NOMO_B;
}

// Forward declarations
bool connectModem();
void restartModemSoft();
void modemAutoCheck();
void initModem();
void initBME();
void initSDCard();
void initOLED();
void appendFile(const char *path, const char *message); // used for outbox and small writes
bool publishOrQueue(const char* topic, const char* payload);
void flushOutbox();
bool checkAndTrimDataFileIfNeeded();
bool getTimeFromModem(char* outBuf, size_t outLen);
bool getTimeString(char* outBuf, size_t outLen);
float readWishnerPressure();
bool connectMQTT();
void attemptRecover();
void i2cScan();
bool initADSRobust(uint8_t address = 0x4B, int retries = 5, int delayMs = 200);
float adsVoltageFromRaw(int16_t raw, int gainSetting);
bool baca16_retry(uint16_t reg, uint16_t &out, int retries = 3, int waitMs = 100);
bool baca32_retry(uint16_t reg, uint32_t &out, int retries = 3, int waitMs = 100);
void updateRelayWithHysteresis(float current);
int getHourFromModem();
int getCurrentHour();
bool getTimeShortString(char* outBuf, size_t outLen);
bool isOLEDTimeWindow();
bool isChargeAllowed();
void updateOLEDDisplay();

// New helpers for CSV logging (now include bytes used fields)
bool ensureCSVExistsForDate(const char* dateYYYYMMDD);
bool appendCSVRow(const char* tanggal, const char* waktu,
                  float waterPressure, float battV, float battPct,
                  float temperature, float humidity, float airPressure,
                  unsigned long bytesCycle, unsigned long long bytesTotal);
String csvFilenameFromDate(const char* dateYYYYMMDD);

// Prototype for SOC lookup
float getSOCfromVoltage(float voltage);

// Watchdog helpers (software watchdog)
void enableWatchdog();
inline void watchdogKick();
void delayWdt(unsigned long ms);
void watchdogCheck();
void checkSystemErrorRestart();

// ---------------- SOC voltage lookup (more detailed) ----------------
// Implemented here so it's available before loop() usage.
float getSOCfromVoltage(float voltage) {
  // Detailed lookup table (12V lead-acid typical OCV -> SOC), descending
  const int N = 101;
  const float V[N] = {
    14.00,13.97,13.95,13.92,13.89,13.87,13.84,13.81,13.79,13.76,13.74,13.71,
    13.68,13.66,13.63,13.61,13.58,13.55,13.53,13.50,13.48,13.45,13.42,13.40,
    13.37,13.35,13.32,13.29,13.27,13.24,13.22,13.19,13.16,13.14,13.11,13.09,
    13.06,13.03,13.01,12.98,12.96,12.93,12.90,12.88,12.85,12.83,12.80,12.77,
    12.75,12.72,12.70,12.67,12.64,12.62,12.59,12.57,12.54,12.51,12.49,12.46,
    12.44,12.41,12.38,12.36,12.33,12.31,12.28,12.25,12.23,12.20,12.18,12.15,
    12.12,12.10,12.07,12.05,12.02,11.99,11.97,11.94,11.92,11.89,11.86,11.84,
    11.81,11.79,11.76,11.73,11.71,11.68,11.66,11.63,11.60,11.58,11.55,11.53,
    11.50,11.47,11.45,11.42,11.40
  };
  const float S[N] = {
    100,99,98,97,96,95,94,93,92,91,
    90,89,88,87,86,85,84,83,82,81,
    80,79,78,77,76,75,74,73,72,71,
    70,69,68,67,66,65,64,63,62,61,
    60,59,58,57,56,55,54,53,52,51,
    50,49,48,47,46,45,44,43,42,41,
    40,39,38,37,36,35,34,33,32,31,
    30,29,28,27,26,25,24,23,22,21,
    20,19,18,17,16,15,14,13,12,11,
    10,9,8,7,6,5,4,3,2,1,0
  };

  if (voltage >= V[0]) return 100.0f;
  if (voltage <= V[N-1]) return 0.0f;

  for (int i = 0; i < N-1; ++i) {
    if (voltage <= V[i] && voltage >= V[i+1]) {
      float t = (voltage - V[i+1]) / (V[i] - V[i+1]);
      float soc = S[i+1] + t * (S[i] - S[i+1]);
      if (soc < 0.0f) soc = 0.0f;
      if (soc > 100.0f) soc = 100.0f;
      return soc;
    }
  }
  return 0.0f;
}

// ---------------- Implementation ----------------

void restartModemSoft() {
  Serial.println("♻  Restarting modem (soft)...");
  if (!modem.restart()) {
    Serial.println("⚠ modem.restart() failed or modem unresponsive.");
  } else {
    Serial.println("🔁 Modem restart command sent.");
  }
  delayWdt(3000);
}

bool connectModem() {
  Serial.println("📶 Waiting for network (up to 60s)...");
  bool regOk = false;
  unsigned long t0 = millis();
  while (millis() - t0 < 60000UL) {
    watchdogKick();
    // Try in 15s chunks to avoid long blocking
    if (modem.waitForNetwork(15000L)) { regOk = true; break; }
    delayWdt(250);
  }
  if (!regOk) {
    Serial.println("❌ Not registered to network.");
    return false;
  }
  if (!modem.isNetworkConnected()) {
    Serial.println("⚠ Network not ready.");
  } else {
    Serial.println("✅ Network registration OK.");
  }
  Serial.printf("🌐 Connecting GPRS with APN '%s' ...\n", apn);
  if (!modem.gprsConnect(apn, gprsUser, gprsPass)) {
    Serial.println("❌ GPRS connection failed.");
    return false;
  }
  Serial.println("✅ GPRS connected. Internet active.");
  return true;
}

void modemAutoCheck() {
  if (millis() - lastModemCheck < modemCheckInterval) return;
  lastModemCheck = millis();

  bool netOk = modem.isNetworkConnected();
  bool gprsOk = modem.isGprsConnected();

  if (netOk && gprsOk) {
    modemConnected = true;
    return;
  }

  Serial.println("⚠ Modem connection problem detected. Trying reconnect...");

  for (int i = 0; i < RECONNECT_TRIES; ++i) {
    Serial.printf("🔁 Reconnect attempt #%d...\n", i + 1);
    if (connectModem()) {
      modemConnected = true;
      Serial.println("✅ Reconnect GPRS success.");
      return;
    }
    delayWdt(2000);
  }

  Serial.println("🔄 Attempt soft restart modem...");
  restartModemSoft();

  if (connectModem()) {
    modemConnected = true;
    Serial.println("✅ Success after modem restart.");
    return;
  }

  modemConnected = false;
  Serial.println("❌ All attempts failed — offline mode (SD-only).");
}

void initModem() {
  Serial.println("🔌 Init Serial for modem...");
  SerialAT.begin(115200, SERIAL_8N1, MODEM_RX, MODEM_TX);
  delayWdt(1200);

  // Pastikan modem di-inisialisasi dengan benar (beberapa board butuh restart/init)
  Serial.println("🔧 Init modem...");
  modem.restart();

  // Bersihkan buffer SerialAT agar parsing AT manual lebih stabil
  while (SerialAT.available()) SerialAT.read();

  Serial.println("⏳ Waiting modem ready (3s)...");
  delayWdt(3000);
  String info = modem.getModemInfo();
  Serial.print("📡 Modem: ");
  Serial.println(info);
  modemConnected = connectModem();
}

void initBME() {
  // retry a few times because sometimes I2C devices are slow after boot
  bool ok = false;
  for (int i = 0; i < 3; ++i) {
    if (bme.begin(0x76)) {
      ok = true;
      break;
    }
    Serial.println("⚠ BME280 not found at 0x76, retrying...");
    delay(200);
  }
  if (!ok) {
    Serial.println("❌ BME280 not found at 0x76 after retries!");
    systemError = true;
  } else {
    Serial.println("✅ BME280 detected.");
  }
}

void initSDCard() {
  Serial.println("💾 Initializing SD Card...");
  if (!SD.begin(SD_CS)) {
    Serial.println("❌ Failed mount SD Card. Check wiring & CS pin.");
    systemError = true;
    return;
  }

  // ensure outbox exists
  if (!SD.exists(OUTBOX_FILE)) {
    File f = SD.open(OUTBOX_FILE, FILE_WRITE);
    if (f) f.close();
  }
}

// Ensure a CSV daily file exists and add header if not.
bool ensureCSVExistsForDate(const char* dateYYYYMMDD) {
  String fname = csvFilenameFromDate(dateYYYYMMDD);
  if (SD.exists(fname.c_str())) return true;
  File f = SD.open(fname.c_str(), FILE_WRITE);
  if (!f) {
    Serial.printf("❌ Failed to create CSV file %s\n", fname.c_str());
    return false;
  }
  // Header for CSV (Excel-friendly) using comma separators
  f.println("Tanggal,Waktu,Water Pressure,Batt V,Batt %,Temperature,Humidity,Air pressure,BytesCycle,BytesTotal");
  f.close();
  Serial.printf("✅ Created CSV file %s with header\n", fname.c_str());
  return true;
}

// Build filename string like "data_YYYYMMDD.csv"
String csvFilenameFromDate(const char* dateYYYYMMDD) {
  String s = String(DATA_FILE_PREFIX) + String(dateYYYYMMDD) + String(DATA_FILE_SUFFIX);
  return s;
}

// Append a single CSV row to daily CSV file (fast append)
// bytesCycle = bytes estimated for this cycle; bytesTotal = cumulative since start
bool appendCSVRow(const char* tanggal, const char* waktu,
                  float waterPressure, float battV, float battPct,
                  float temperature, float humidity, float airPressure,
                  unsigned long bytesCycle, unsigned long long bytesTotal) {
  // tanggal expected as "DD/MM/YYYY"
  // Convert to YYYYMMDD for filename
  char dateYMD[16] = {0};
  int d = 0, m = 0, y = 0;
  if (tanggal && strlen(tanggal) >= 8) {
    // try sscanf
    if (sscanf(tanggal, "%d/%d/%d", &d, &m, &y) == 3) {
      snprintf(dateYMD, sizeof(dateYMD), "%04d%02d%02d", y, m, d);
    } else {
      // fallback: localtime if available
      struct tm ti;
      if (getLocalTime(&ti)) {
        snprintf(dateYMD, sizeof(dateYMD), "%04d%02d%02d", ti.tm_year + 1900, ti.tm_mon + 1, ti.tm_mday);
      } else {
        strncpy(dateYMD, "unknown", sizeof(dateYMD)-1);
      }
    }
  } else {
    struct tm ti;
    if (getLocalTime(&ti)) {
      snprintf(dateYMD, sizeof(dateYMD), "%04d%02d%02d", ti.tm_year + 1900, ti.tm_mon + 1, ti.tm_mday);
    } else {
      strncpy(dateYMD, "unknown", sizeof(dateYMD)-1);
    }
  }

  String fname = csvFilenameFromDate(dateYMD);
  if (!SD.exists(fname.c_str())) {
    if (!ensureCSVExistsForDate(dateYMD)) {
      Serial.println("⚠ Failed to ensure CSV exists; abort append.");
      return false;
    }
  }

  File f = SD.open(fname.c_str(), FILE_APPEND);
  if (!f) {
    Serial.printf("❌ Failed to open CSV %s for append\n", fname.c_str());
    return false;
  }

  // Format CSV line (comma separated). Use integer formatting for bytes.
  char linebuf[320];
  int n = snprintf(linebuf, sizeof(linebuf), "%s,%s,%.3f,%.2f,%.1f,%.2f,%.2f,%.2f,%lu,%llu\n",
                   tanggal, waktu,
                   waterPressure, battV, battPct, temperature, humidity, airPressure,
                   (unsigned long)bytesCycle, (unsigned long long)bytesTotal);
  if (n < 0) {
    f.close();
    return false;
  }
  f.print(linebuf);
  f.close();
  return true;
}

void initOLED() {
  // retry a couple times for robustness
  bool ok = false;
  for (int i = 0; i < 3; ++i) {
    if (display.begin(0x3C, true)) {
      ok = true;
      break;
    }
    Serial.println("⚠ OLED SH1106 init failed, retrying...");
    delay(200);
  }
  if (!ok) {
    Serial.println("❌ OLED SH1106 init failed after retries!");
    systemError = true;
    oledInitialized = false;
    return;
  }
  display.clearDisplay();
  display.display();
  Serial.println("✅ OLED initialized.");
  oledInitialized = true;
}

void appendFile(const char *path, const char *message) {
  File file = SD.open(path, FILE_APPEND);
  if (!file) {
    Serial.println("⚠ Failed open file append — trying create new...");
    file = SD.open(path, FILE_WRITE);
    if (!file) {
      Serial.println("❌ Failed create file as well.");
      return;
    }
  }
  file.print(message);
  file.close();
  Serial.print("✅ Saved to SD: ");
  Serial.println(path);
}

bool getTimeFromModem(char* outBuf, size_t outLen) {
  // Query modem for clock and parse robustly by reading line by line
  SerialAT.println("AT+CCLK?");
  unsigned long start = millis();
  String line;
  String found = "";
  while (millis() - start < 3000) {
    if (SerialAT.available()) {
      line = SerialAT.readStringUntil('\n');
      line.trim();
      if (line.length() == 0) continue;
      Serial.print("DEBUG +CCLK line: ");
      Serial.println(line);
      if (line.indexOf("+CCLK:") != -1) {
        found = line;
        break;
      } else if (line.indexOf('"') != -1 && line.indexOf(":") == -1 && line.indexOf("OK") == -1) {
        // Some modems reply time in a quoted string line
        found = line;
        break;
      }
    }
  }
  if (found.length() == 0) return false;

  int q1 = found.indexOf('"');
  int q2 = found.indexOf('"', q1 + 1);
  String timestr;
  if (q1 != -1 && q2 != -1) {
    timestr = found.substring(q1 + 1, q2);
  } else {
    // try to extract digits presumptively
    timestr = found;
  }
  timestr.trim();
  if (timestr.length() >= 17) {
    // attempt parse "yy/MM/dd,hh:mm:ss+zz" or "yy/MM/dd,hh:mm:ss"
    int firstSlash = timestr.indexOf('/');
    int secondSlash = timestr.indexOf('/', firstSlash + 1);
    int comma = timestr.indexOf(',');
    if (firstSlash == -1 || secondSlash == -1 || comma == -1) {
      // fallback: try split by space
      int sp = timestr.indexOf(' ');
      if (sp > 0) {
        String d = timestr.substring(0, sp);
        String t = timestr.substring(sp + 1);
        int yyyy = 2000 + d.substring(0,2).toInt();
        snprintf(outBuf, outLen, "%s %s", d.c_str(), t.c_str());
        return true;
      }
      return false;
    }
    String yy = timestr.substring(0, firstSlash);
    String mm = timestr.substring(firstSlash + 1, secondSlash);
    String dd = timestr.substring(secondSlash + 1, comma);
    String clock = timestr.substring(comma + 1, comma + 9);
    int yyyy = 2000 + yy.toInt();
    snprintf(outBuf, outLen, "%02d/%02d/%04d %s", dd.toInt(), mm.toInt(), yyyy, clock.c_str());
    return true;
  }
  return false;
}

bool getTimeString(char* outBuf, size_t outLen) {
  struct tm timeinfo;
  if (getLocalTime(&timeinfo)) {
    char buf[32];
    strftime(buf, sizeof(buf), "%d/%m/%Y %H:%M:%S", &timeinfo);
    strncpy(outBuf, buf, outLen);
    outBuf[outLen - 1] = '\0';
    return true;
  }
  if (getTimeFromModem(outBuf, outLen)) {
    return true;
  }
  strncpy(outBuf, "Gagal ambil waktu", outLen);
  outBuf[outLen - 1] = '\0';
  return false;
}

// --- NEW HELPERS: modem hour extraction and safe short-time string ---

// Return hour 0..23 from modem AT+CCLK? response, or -1 if cannot parse
int getHourFromModem() {
  Serial.println("Attempting to get hour from modem (AT+CCLK?)...");
  SerialAT.println("AT+CCLK?");
  unsigned long start = millis();
  String line;
  String found = "";
  while (millis() - start < 1500) {
    if (SerialAT.available()) {
      line = SerialAT.readStringUntil('\n');
      line.trim();
      if (line.length() == 0) continue;
      Serial.print("DEBUG +CCLK line: ");
      Serial.println(line);
      if (line.indexOf("+CCLK:") != -1) {
        found = line;
        break;
      } else if (line.indexOf('"') != -1 && line.indexOf("OK") == -1) {
        found = line;
        break;
      }
    }
  }
  if (found.length() == 0) {
    Serial.println("No CCLK response found.");
    return -1;
  }

  int q1 = found.indexOf('"');
  int q2 = found.indexOf('"', q1 + 1);
  String timestr;
  if (q1 != -1 && q2 != -1) timestr = found.substring(q1 + 1, q2);
  else timestr = found;
  timestr.trim();

  int comma = timestr.indexOf(',');
  String timePart;
  if (comma != -1) timePart = timestr.substring(comma + 1);
  else {
    int sp = timestr.indexOf(' ');
    if (sp != -1) timePart = timestr.substring(sp + 1);
    else timePart = timestr;
  }
  timePart.trim();
  int c1 = timePart.indexOf(':');
  if (c1 == -1) {
    Serial.println("Time part has no ':' separator.");
    return -1;
  }
  String hh = timePart.substring(0, c1);
  hh.trim();
  int hour = hh.toInt();
  if (hour < 0 || hour > 23) {
    Serial.printf("Parsed hour out of range: %d\n", hour);
    return -1;
  }
  Serial.printf("Modem hour parsed: %d\n", hour);
  return hour;
}

// Create safe HH:MM:SS string either from NTP (localtime) or fallback to modem parsing
bool getTimeShortString(char* outBuf, size_t outLen) {
  struct tm timeinfo;
  if (getLocalTime(&timeinfo)) {
    snprintf(outBuf, outLen, "%02d:%02d:%02d", timeinfo.tm_hour, timeinfo.tm_min, timeinfo.tm_sec);
    return true;
  }
  // fallback: try getTimeFromModem (which returns "DD/MM/YYYY HH:MM:SS" or similar)
  char modBuf[64] = {0};
  if (getTimeFromModem(modBuf, sizeof(modBuf))) {
    String s = String(modBuf);
    s.trim();
    int sp = s.indexOf(' ');
    String timePart;
    if (sp != -1) timePart = s.substring(sp + 1);
    else timePart = s;
    timePart.trim();
    int c1 = timePart.indexOf(':');
    if (c1 == -1) return false;
    int c2 = timePart.indexOf(':', c1 + 1);
    int h = 0, m = 0, sec = 0;
    if (c2 == -1) {
      h = timePart.substring(0, c1).toInt();
      m = timePart.substring(c1 + 1).toInt();
      sec = 0;
    } else {
      h = timePart.substring(0, c1).toInt();
      m = timePart.substring(c1 + 1, c2).toInt();
      sec = timePart.substring(c2 + 1).toInt();
    }
    if (h < 0 || h > 23) return false;
    snprintf(outBuf, outLen, "%02d:%02d:%02d", h, m, sec);
    return true;
  }
  return false;
}

// Return true if current local hour is between 06..23 (OLED behavior)
// Uses NTP first, falls back to cached modem hour (getHourFromModem) to avoid frequent AT+CCLK.
const bool DEFAULT_OLED_FALLBACK = false; // change to false for production (keep OLED off when time unknown)
bool isOLEDTimeWindow() {
  int hour = getCurrentHour();

  if (hour < 0) {
    Serial.println("isOLEDTimeWindow: no time available, using fallback policy.");
    return DEFAULT_OLED_FALLBACK;
  }

  // ON between 08:00..16:59 originally — keep as behavior in original code
  bool on = (hour >= 8 && hour < 24);
  Serial.printf("isOLEDTimeWindow: returning %d (hour=%d)\n", on ? 1 : 0, hour);
  return on;
}

// Get current hour using NTP/localtime if available, else use cached modem hour (refresh every HOUR_CACHE_MS)
int getCurrentHour() {
  struct tm timeinfo;
  if (getLocalTime(&timeinfo)) {
    int h = timeinfo.tm_hour;
    // update cache as well
    cachedHour = h;
    cachedHourMillis = millis();
    return h;
  }
  unsigned long now = millis();
  if ((cachedHour >= 0) && (now - cachedHourMillis <= HOUR_CACHE_MS)) {
    return cachedHour;
  }
  // cache miss => query modem (blocking)
  int h = getHourFromModem();
  if (h >= 0) {
    cachedHour = h;
    cachedHourMillis = now;
  }
  return h;
}

// Create safe policy: allow charging only between 06:00..17:59. If time unknown, default to allow.
bool isChargeAllowed() {
  int hour = getCurrentHour();
  if (hour < 0) {
    // time not available: policy - allow charging (change to false to default OFF)
    return true;
  }
  if (hour >= 6 && hour < 18) return true;
  return false;
}

float readWishnerPressure() {
  float v_sensor;
  if (manualMode) {
    if (Serial.available()) {
      v_sensor = Serial.parseFloat();
      Serial.print("Manual sensor voltage: ");
      Serial.print(v_sensor, 3);
      Serial.println(" V");
    } else {
      return wishnerPressure;
    }
  } else {
    if (!adsDetected) {
      Serial.println("⚠ ADS1115 tidak terdeteksi, menggunakan last wishner value.");
      return wishnerPressure;
    }
    int16_t adcValue = ads.readADC_SingleEnded(0);
    // ADS1115 full-scale untuk GAIN_ONE = ±4.096V
    float v_adc = adsVoltageFromRaw(adcValue, GAIN_ONE);
    v_sensor = v_adc / dividerRatio;
  }
  float pressure = (v_sensor - SENSOR_VMIN) *
                   (PRESSURE_MAX - PRESSURE_MIN) /
                   (SENSOR_VMAX - SENSOR_VMIN) + PRESSURE_MIN;
  pressure += pressureOffset;
  if (pressure < 0) pressure = 0;
  return pressure;
}

bool connectMQTT() {
  if (mqttClient.connected()) return true;

  unsigned long now = millis();
  if (now - lastMQTTAttempt < MQTT_RETRY_INTERVAL) {
    return false; // guard: jangan reconnect terlalu sering
  }
  lastMQTTAttempt = now;

  Serial.println("🔌 Connecting to MQTT (Basic Auth)...");
  bool status = mqttClient.connect(
    mqtt_clientId,
    mqttUsername,
    mqttPassword
  );

  if (status) {
    Serial.println("✅ MQTT Connected (Basic)");
  } else {
    Serial.print("❌ MQTT Failed, rc=");
    Serial.println(mqttClient.state());
  }
  return status;
}

// Publish or queue on SD if offline / failure
bool publishOrQueue(const char* topic, const char* payload) {
  if (modem.isGprsConnected()) {
    if (!mqttClient.connected()) {
      if (!connectMQTT()) {
        // failed to connect; will queue below
      }
    }
    if (mqttClient.connected()) {
      mqttClient.loop();
      if (mqttClient.publish(topic, payload)) {
        return true;
      } else {
        Serial.println("⚠ publish failed, queuing to SD");
        mqttClient.disconnect();
      }
    }
  }
  // queue format: topic|payload\n
  char qline[512];
  const size_t maxPayload = 400;
  if (strlen(payload) > maxPayload) {
    // copy truncated payload
    char truncated[maxPayload + 1];
    strncpy(truncated, payload, maxPayload);
    truncated[maxPayload] = '\0';
    snprintf(qline, sizeof(qline), "%s|%s\n", topic, truncated);
  } else {
    snprintf(qline, sizeof(qline), "%s|%s\n", topic, payload);
  }
  appendFile(OUTBOX_FILE, qline);
  return false;
}

// Outbox non-blocking helpers (offset-based)
const char* OUTBOX_IDX_FILE = "/outbox.idx";

uint64_t readOutboxOffset() {
  if (!SD.exists(OUTBOX_IDX_FILE)) return 0ULL;
  File f = SD.open(OUTBOX_IDX_FILE, FILE_READ);
  if (!f) return 0ULL;
  String s = f.readStringUntil('\n');
  f.close();
  s.trim();
  if (s.length() == 0) return 0ULL;
  return (uint64_t) s.toInt(); // offset fits in 32-bit for typical SD usage
}

void writeOutboxOffset(uint64_t off) {
  File f = SD.open(OUTBOX_IDX_FILE, FILE_WRITE);
  if (!f) return;
  f.seek(0);
  f.print(String((unsigned long)off));
  f.print("\n");
  f.close();
}

// Attempt to flush outbox: non-blocking (reads from offset, no full-file copy)
void flushOutbox() {
  static unsigned long lastOutboxFlushTry = 0;
  if (millis() - lastOutboxFlushTry < 30000UL) return;
  lastOutboxFlushTry = millis();

  if (!SD.exists(OUTBOX_FILE)) return;

  // Only try flushing when we have connectivity (otherwise we just keep queuing)
  if (!modem.isGprsConnected()) return;
  if (!mqttClient.connected()) {
    if (!connectMQTT()) return;
  }

  File f = SD.open(OUTBOX_FILE, FILE_READ);
  if (!f) return;

  uint64_t off = readOutboxOffset();
  if (off > (uint64_t)f.size()) off = 0ULL;
  f.seek((uint32_t)off);

  const size_t LINE_BUF = 512;
  char lineBuf[LINE_BUF];
  int sent = 0;
  unsigned long flushStart = millis();
  const unsigned long FLUSH_BUDGET_MS = 1500UL; // keep loop responsive

  while (f.available() && sent < OUTBOX_FLUSH_BATCH) {
    watchdogKick();
    if (millis() - flushStart > FLUSH_BUDGET_MS) break;
    size_t r = f.readBytesUntil('\n', lineBuf, LINE_BUF - 1);
    if (r == 0) break;
    lineBuf[r] = '\0';
    // strip CR
    if (r > 0 && lineBuf[r - 1] == '\r') lineBuf[r - 1] = '\0';

    char *sep = strchr(lineBuf, '|');
    if (!sep) {
      // malformed line: skip it but still advance offset
      off = (uint64_t)f.position();
      sent++;
      continue;
    }
    *sep = '\0';
    const char *topic = lineBuf;
    const char *payload = sep + 1;

    mqttClient.loop();
    if (!mqttClient.connected()) {
      // connection dropped mid-flush; stop and keep offset where it was
      break;
    }

    bool ok = mqttClient.publish(topic, payload);
    if (!ok) {
      // publish failed; stop here and retry later
      break;
    }

    off = (uint64_t)f.position(); // advance to next line start
    sent++;
  }

  f.close();

  // Persist new offset
  writeOutboxOffset(off);

  // If we've consumed the file, clear it
  File f2 = SD.open(OUTBOX_FILE, FILE_READ);
  if (f2) {
    uint32_t sz = f2.size();
    f2.close();
    if (off >= (uint64_t)sz) {
      SD.remove(OUTBOX_FILE);
      SD.remove(OUTBOX_IDX_FILE);
      Serial.println("✅ Outbox fully flushed.");
    }
  }
}

// Check approximate SD capacity and trim oldest daily CSV files if exceeding threshold.
bool checkAndTrimDataFileIfNeeded() {
  // If capacity not configured, do nothing
  if (APPROX_SD_CAPACITY_BYTES == 0) return true;

  // Collect CSV data_* files
  struct FileInfo { String name; uint64_t size; long dateKey; bool valid; };
  const int MAX_FILES = 128;
  FileInfo files[MAX_FILES];
  int fileCount = 0;

  File root = SD.open("/");
  if (!root) return true;

  File entry;
  while ((entry = root.openNextFile())) {
    String nm = entry.name(); // name without leading slash
    if (nm.startsWith(DATA_FILE_PREFIX) && nm.endsWith(DATA_FILE_SUFFIX)) {
      if (fileCount < MAX_FILES) {
        files[fileCount].name = nm;
        files[fileCount].size = entry.size();
        // parse date key if possible: data_YYYYMMDD.csv
        long key = 0;
        if (nm.length() >= (int)(strlen(DATA_FILE_PREFIX) + 8 + strlen(DATA_FILE_SUFFIX))) {
          String dt = nm.substring(strlen(DATA_FILE_PREFIX), strlen(DATA_FILE_PREFIX) + 8);
          key = dt.toInt();
        }
        files[fileCount].dateKey = key;
        files[fileCount].valid = true;
        fileCount++;
      }
    }
    entry.close();
  }
  root.close();

  uint64_t totalSize = 0;
  for (int i = 0; i < fileCount; ++i) totalSize += files[i].size;

  uint64_t threshold = (uint64_t)(APPROX_SD_CAPACITY_BYTES * SD_FULL_THRESHOLD);
  if (totalSize <= threshold) return true;

  Serial.println("⚠ SD capacity threshold exceeded. Deleting oldest CSV files until below threshold...");

  // Delete oldest based on dateKey (or fallback to lexicographic)
  while (totalSize > threshold && fileCount > 0) {
    int oldestIdx = -1;
    long oldestKey = LONG_MAX;
    for (int i = 0; i < fileCount; ++i) {
      if (!files[i].valid) continue;
      long k = files[i].dateKey;
      if (k == 0) {
        // fallback lexicographic ordering
        if (oldestIdx == -1) { oldestIdx = i; oldestKey = 0; }
        else if (files[i].name < files[oldestIdx].name) oldestIdx = i;
      } else {
        if (k < oldestKey) { oldestKey = k; oldestIdx = i; }
      }
    }
    if (oldestIdx == -1) break;

    String delName = files[oldestIdx].name;
    if (SD.remove(delName.c_str())) {
      Serial.printf("🗑 Deleted old CSV: %s (size=%llu)\n", delName.c_str(), files[oldestIdx].size);
      totalSize -= files[oldestIdx].size;
      files[oldestIdx].valid = false;
    } else {
      Serial.printf("⚠ Failed delete %s\n", delName.c_str());
      // mark invalid to avoid infinite loop
      files[oldestIdx].valid = false;
      fileCount--;
    }
  }

  Serial.println("✅ SD trimming complete.");
  return true;
}

void attemptRecover() {
  if (!systemError) return;
  if (millis() - lastRecoverAttempt < RECOVER_INTERVAL) return;
  lastRecoverAttempt = millis();
  recoverAttempts++;
  Serial.printf("🔁 Attempting recovery #%d...\n", recoverAttempts);
  // try re-init subsystems
  systemError = false;
  initBME();
  initSDCard();
  initOLED();
  // try modem re-init if needed
  if (!modem.isGprsConnected()) {
    initModem();
  }
  // try re-init ADS if previously failed
  if (!adsDetected) {
    adsDetected = initADSRobust(0x4B, 3, 300);
  }
  if (!systemError && adsDetected) {
    Serial.println("✅ Recovery succeeded.");
    digitalWrite(RELAY_SYS_PIN, HIGH);
    recoverAttempts = 0;
  } else {
    Serial.println("⚠ Recovery attempt failed.");
  }
  if (recoverAttempts >= MAX_RECOVER_ATTEMPTS) {
    Serial.println("❌ Max recovery attempts reached. Restarting MCU...");
    delayWdt(2000);
    ESP.restart();
  }
}

// ---------------- I2C Scanner & ADS init helpers ----------------

void i2cScan() {
  Serial.println("I2C Scanner:");
  bool found = false;
  for (uint8_t addr = 1; addr < 127; ++addr) {
    Wire.beginTransmission(addr);
    if (Wire.endTransmission() == 0) {
      Serial.print(" - Found device at 0x");
      if (addr < 16) Serial.print('0');
      Serial.println(addr, HEX);
      found = true;
    }
  }
  if (!found) Serial.println(" - No I2C devices found");
}

bool initADSRobust(uint8_t address, int retries, int delayMs) {
  Serial.printf("Init ADS1115 at 0x%02X ...\n", address);
  for (int i = 0; i < retries; ++i) {
    if (ads.begin(address)) {
      // NOTE: consider using GAIN_TWOTHIRDS if you expect >4.096V on ADC input
      ads.setGain(GAIN_ONE); // pilih gain yang sesuai (GAIN_ONE => ±4.096V)
      Serial.println("✅ ADS1115 detected and configured (GAIN_ONE).");
      return true;
    }
    Serial.printf("⚠ ADS1115 not found, attempt %d/%d\n", i + 1, retries);
    i2cScan();
    delay(delayMs);
  }
  Serial.println("❌ ADS1115 init failed after retries");
  return false;
}

float adsVoltageFromRaw(int16_t raw, int gainSetting) {
  float fs = 4.096f; // default for GAIN_ONE
  switch (gainSetting) {
    case GAIN_TWOTHIRDS: fs = 6.144f; break;
    case GAIN_ONE:       fs = 4.096f; break;
    case GAIN_TWO:       fs = 2.048f; break;
    case GAIN_FOUR:      fs = 1.024f; break;
    case GAIN_EIGHT:     fs = 0.512f; break;
    case GAIN_SIXTEEN:   fs = 0.256f; break;
    default:             fs = 4.096f; break;
  }
  float v = (raw * fs) / 32767.0f;
  return v;
}

// ---------------- PZEM Modbus helpers ----------------

// read 16-bit with retries (FC4 or FC3 depending on flag)
bool baca16_retry(uint16_t reg, uint16_t &out, int retries, int waitMs) {
  for (int i = 0; i < retries; ++i) {
    uint8_t res;
    if (PZEM_USE_HOLDING_REGISTERS) res = node.readHoldingRegisters(reg, 1);
    else res = node.readInputRegisters(reg, 1);
    if (res == node.ku8MBSuccess) {
      out = node.getResponseBuffer(0);
      return true;
    }
    Serial.printf("Modbus read16 fail code=%u (reg=0x%04X, attempt %d)\n", res, reg, i+1);
    delay(waitMs);
  }
  return false;
}

// read 32-bit with retries and corrected endianness
bool baca32_retry(uint16_t reg, uint32_t &out, int retries, int waitMs) {
  for (int i = 0; i < retries; ++i) {
    uint8_t res;
    if (PZEM_USE_HOLDING_REGISTERS) res = node.readHoldingRegisters(reg, 2);
    else res = node.readInputRegisters(reg, 2);
    if (res == node.ku8MBSuccess) {
      uint32_t high = node.getResponseBuffer(0);
      uint32_t low  = node.getResponseBuffer(1);
      out = (high << 16) | low; // asumsi high-word di register pertama (cek dokumentasi PZEM Anda)
      return true;
    }
    Serial.printf("Modbus read32 fail code=%u (reg=0x%04X, attempt %d)\n", res, reg, i+1);
    delay(waitMs);
  }
  return false;
}

// ---------------- Relay update with schedule-aware hysteresis ----------------

// Force relay OFF if schedule disallows charging (18:00..06:00),
// otherwise apply usual hysteresis but still obey min toggle interval.
void updateRelayWithHysteresis(float current) {
  unsigned long now = millis();

  // If schedule forbids charging, ensure relay is OFF
  if (!isChargeAllowed()) {
    if (relayChargeState) {
      relayChargeState = false;
      digitalWrite(RELAY_CHARGE_PIN, LOW);
      lastRelayChange = now;
      Serial.println("⚡ Relay CHARGE OFF (scheduled)");
    }
    return;
  }

  // If allowed by schedule, normal hysteresis logic
  if (!relayChargeState) {
    if (current > CHARGE_ON_TH && (now - lastRelayChange) > RELAY_MIN_TOGGLE_MS) {
      relayChargeState = true;
      digitalWrite(RELAY_CHARGE_PIN, HIGH);
      lastRelayChange = now;
      Serial.println("🔋 Relay CHARGE ON");
    }
  } else {
    if (current < CHARGE_OFF_TH && (now - lastRelayChange) > RELAY_MIN_TOGGLE_MS) {
      relayChargeState = false;
      digitalWrite(RELAY_CHARGE_PIN, LOW);
      lastRelayChange = now;
      Serial.println("⚡ Relay CHARGE OFF");
    }
  }
}

// ---------------- OLED helpers ----------------

// Render one page depending on oledPage variable (safe footer using getTimeShortString)
void updateOLEDDisplay() {
  if (!oledInitialized) return;

  bool shouldOn = isOLEDTimeWindow();
  if (!shouldOn) {
    // If OLED was previously on, clear it now
    if (oledWasOn) {
      display.clearDisplay();
      display.display();
      oledWasOn = false;
      Serial.println("ℹ OLED turned OFF due to schedule.");
    }
    return;
  }

  // It's within allowed hours
  oledWasOn = true;

  // Render content based on page index
  display.clearDisplay();
  display.setTextSize(1);
  display.setTextColor(SH110X_WHITE);

  switch (oledPage) {
    case 0:
      display.setCursor(0, 0);
      display.setTextSize(1);
      display.print("PT. SCTK");
      display.setCursor(0, 20);
      display.setTextSize(1);
      display.print("Pressure Monitoring");
      display.setCursor(0, 40);
      display.setTextSize(1);
      display.print("By: Desta & Hanif");
      break;

    case 1:
      // label
      display.setTextSize(2);
      display.setCursor(0, 0);
      display.print("Tekanan:");
      // value (bigger)
      display.setCursor(0, 20);
      display.setTextSize(2);
      {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.3f bar", wishnerPressure);
        display.print(buf);
      }
      display.setTextSize(1);
      break;

    case 2:
      // Velocity page (new)
      display.setTextSize(2);
      display.setCursor(0, 0);
      display.print("Velocity:");
      display.setCursor(0, 20);
      display.setTextSize(2);
      {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.3f m/s", velocity);
        display.print(buf);
      }
      display.setTextSize(1);
      break;

    case 3:
      display.setTextSize(2);
      display.setCursor(0, 0);
      display.print("Suhu:");
      display.setCursor(0, 20);
      display.setTextSize(2);
      {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.2f C", suhu);
        display.print(buf);
      }
      display.setTextSize(1);
      break;

    case 4:
      display.setTextSize(2);
      display.setCursor(0, 0);
      display.print("Kelembapan:");
      display.setCursor(0, 20);
      display.setTextSize(2);
      {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.2f %%", kelembapan);
        display.print(buf);
      }
      display.setTextSize(1);
      break;

    case 5:
      display.setTextSize(2);
      display.setCursor(0, 0);
      display.print("V Batt:");
      display.setCursor(0, 20);
      display.setTextSize(2);
      {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.2f V", pzemVoltage);
        display.print(buf);
      }
      display.setTextSize(1);
      break;

    case 6:
      // Battery SOC page
      display.setTextSize(2);
      display.setCursor(0, 0);
      display.print("Batt:");
      display.setCursor(0, 20);
      display.setTextSize(2);
      {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.0f %%", batterySOC);
        display.print(buf);
      }
      display.setTextSize(1);
      break;

    default:
      display.setCursor(0, 0);
      display.print("PT. SCTK");
      break;
  }

  // Footer timestamp (safe)
  char shortTime[16] = {0};
  if (getTimeShortString(shortTime, sizeof(shortTime))) {
    display.setCursor(0, 56);
    display.setTextSize(1);
    display.print(shortTime);
  } else {
    display.setCursor(0, 56);
    display.setTextSize(1);
    display.print("---:--:--");
  }

  display.display();
}

// ---------------- Watchdog (software) implementation ----------------

void enableWatchdog() {
  // Initialize the software watchdog baseline
  wdtLastKick = millis();
  Serial.printf("✅ Software WDT initialized (timeout=%us)\n", WDT_TIMEOUT_S);
}

inline void watchdogKick() {
  wdtLastKick = millis();
}



// Delay helper that keeps the software watchdog alive during long operations
void delayWdt(unsigned long ms) {
  unsigned long start = millis();
  while (millis() - start < ms) {
    watchdogKick();
    // keep MQTT housekeeping if connected
    mqttClient.loop();
    delay(10);
  }
}
void watchdogCheck() {
  if (wdtLastKick == 0) return;
  unsigned long now = millis();
  if ((now - wdtLastKick) > (unsigned long)(WDT_TIMEOUT_S * 1000UL)) {
    Serial.println("❗ Software WDT timeout -> restarting MCU.");
    delay(200);
    ESP.restart();
  }
}

void checkSystemErrorRestart() {
  if (!systemError) {
    systemErrorSince = 0;
    return;
  }
  if (systemErrorSince == 0) systemErrorSince = millis();
  if ((millis() - systemErrorSince) >= SYSTEM_ERROR_RESTART_MS) {
    Serial.println("❗ systemError persisted too long -> restarting MCU.");
    delay(200); // allow serial prints flush
    ESP.restart();
  }
}

// ---------------- Setup & Loop ----------------

void setup() {
  Serial.begin(115200);
  delay(100);

  pinMode(RELAY_SYS_PIN, OUTPUT);
  digitalWrite(RELAY_SYS_PIN, LOW);

  pinMode(RELAY_CHARGE_PIN, OUTPUT);
  digitalWrite(RELAY_CHARGE_PIN, LOW); // pastikan relay charging mati di awal

  pinMode(EN_RS485, OUTPUT);
  digitalWrite(EN_RS485, LOW);

  Serial.println("=== START ESP32 SIM7600G WEATHER LOGGER + WISHNER + RELAY (ThingsBoard) + PZEM-017 ===");

  // init modem (Serial1)
  initModem();
  mqttClient.setServer(mqtt_server, mqtt_port);
  mqttClient.setKeepAlive(120);

  // init Modbus on Serial2 (RS485)
  Serial2.begin(9600, SERIAL_8N1, 27, 25); // RX=27, TX=25
  node.begin(SLAVE_ADDR, Serial2);
  node.preTransmission(preTransmission);
  node.postTransmission(postTransmission);

  // IMPORTANT: init I2C bus BEFORE initializing BME280 / OLED / ADS1115
  Wire.begin(21, 22);   // SDA=21, SCL=22
  delay(200);           // give bus a short time to stabilize

  // optional debug: scan devices at boot
  i2cScan();

  // Initialize I2C devices (BME, OLED) now that Wire is started
  initBME();
  initOLED();

  // ADS1115 robust init (try common addresses 0x48..0x4B)
  uint8_t detectedAddr = 0;
  for (uint8_t a = 0x48; a <= 0x4B; ++a) {
    Wire.beginTransmission(a);
    if (Wire.endTransmission() == 0) { detectedAddr = a; break; }
  }
  if (detectedAddr) {
    adsDetected = initADSRobust(detectedAddr, 5, 200);
  } else {
    // fallback to user-provided address 0x4B
    adsDetected = initADSRobust(0x4B, 5, 200);
  }

  if (!adsDetected) {
    Serial.println("⚠ ADS1115 tidak terdeteksi pada startup. Sistem akan coba recovery periodik.");
    // don't set systemError immediately; allow system to continue (optional)
  }

  // SD uses SPI, can be initialized after I2C devices
  initSDCard();

  // IMPORTANT: call configTime AFTER having a working network/Internet connection.
  // If initModem() connected GPRS earlier, it's fine; otherwise ensure modem is connected.
  configTime(gmtOffset_sec, daylightOffset_sec, ntpServer);
  delayWdt(2000);

  if (!systemError) {
    digitalWrite(RELAY_SYS_PIN, HIGH);
    Serial.println("✅ Semua inisialisasi sukses, relay AKTIF.");
  } else {
    digitalWrite(RELAY_SYS_PIN, LOW);
    Serial.println("❌ Inisialisasi gagal, relay OFF. Akan coba recovery periodik.");
  }

  // seed random with modem IMEI (manual hash)
  String imei = modem.getIMEI();
  uint32_t imeiHash = 0;
  const char *p = imei.c_str();
  while (*p) { imeiHash = imeiHash * 131u + (uint8_t)(*p); p++; }
  randomSeed((uint32_t)micros() ^ imeiHash);

  // ENABLE SOFTWARE WATCHDOG (pastikan ini dipanggil sekali)
  enableWatchdog();

  // Ensure SD trimming if near full on startup
  checkAndTrimDataFileIfNeeded();

  // Initialize OLED cycle timer
  lastOLEDCycle = millis();
  oledPage = 0;
  // initial update (if within time)
  updateOLEDDisplay();
}

void loop() {
  // Cheap scheduled enforcement: ensure relay OFF immediately when schedule forbids charging.
  // This avoids waiting up to timerDelay for sampling to catch schedule change.
  if (!isChargeAllowed() && relayChargeState) {
    relayChargeState = false;
    digitalWrite(RELAY_CHARGE_PIN, LOW);
    lastRelayChange = millis();
    Serial.println("⚡ Relay CHARGE OFF (scheduled enforcement)");
  }

  // Kick the software watchdog early in the loop so long blocking ops still have short window
  watchdogKick();
  watchdogCheck();

  if (systemError) {
    digitalWrite(RELAY_SYS_PIN, LOW);
    Serial.println("❌ System error detected in loop, relay OFF. Will attempt recovery periodically.");
    attemptRecover();
    // cek apakah systemError berlangsung terlalu lama, lalu restart jika perlu
    checkSystemErrorRestart();
    delay(1000);
    return;
  }

  modemAutoCheck();

  // Always call mqttClient.loop() frequently so PubSubClient can handle keepalive/acks
  mqttClient.loop();

  // try flush outbox if online
  if (modem.isGprsConnected()) {
    flushOutbox();
  }

  // sensor sampling (every timerDelay)
  if ((millis() - lastTime) > timerDelay) {
    unsigned long now = millis();
    unsigned long dt_ms = now - lastTime;
    lastTime = now;

    // BME readings
    suhu = bme.readTemperature();
    kelembapan = bme.readHumidity();
    tekanan = bme.readPressure() / 100.0F;

    if (isnan(suhu) || isnan(kelembapan) || isnan(tekanan)) {
      Serial.println("❌ BME280 read error. Marking systemError and scheduling recovery.");
      digitalWrite(RELAY_SYS_PIN, LOW);
      systemError = true;
      lastRecoverAttempt = millis(); // prompt immediate attemptRecover
      return;
    }

    // Wishner
    wishnerPressure = readWishnerPressure();
    if (isnan(wishnerPressure)) {
      Serial.println("❌ Wishner read error. Marking systemError and scheduling recovery.");
      digitalWrite(RELAY_SYS_PIN, LOW);
      systemError = true;
      lastRecoverAttempt = millis();
      return;
    }

    // --- PZEM read (update global pzemVoltage/pzemCurrent) ---
    uint16_t v16;
    uint32_t v32;
    if (baca16_retry(0x0000, v16, 2, 100)) {
      pzemVoltage = v16 / 100.0;
    } else {
      Serial.println("Gagal baca voltage PZEM.");
      pzemVoltage = 0.0;
    }
    if (baca16_retry(0x0001, v16, 2, 100)) {
      pzemCurrent = v16 / 100.0;
    } else {
      Serial.println("Gagal baca current PZEM.");
      pzemCurrent = 0.0;
    }
    if (baca32_retry(0x0002, v32, 2, 100)) {
      pzemPower = v32 / 10.0;
    } else {
      Serial.println("Gagal baca power PZEM.");
      pzemPower = 0.0;
    }
    if (baca32_retry(0x0004, v32, 2, 100)) {
      pzemEnergy = (float)v32;
    } else {
      Serial.println("Gagal baca energy PZEM.");
      pzemEnergy = 0.0;
    }

    // apply offset
    pzemCurrent += currentOffset;

    // update charging relay with hysteresis and schedule-aware
    updateRelayWithHysteresis(pzemCurrent);

    // -------------------------
    // VOLTAGE FILTERING + SLOPE-BASED STATE DETECTION + SOC BLENDING
    // -------------------------
    if (lastVoltageMillis == 0) {
      // first-time init
      filteredVoltage = pzemVoltage;
      lastVoltageMillis = now;
      batterySOC = getSOCfromVoltage(filteredVoltage);
    } else {
      float dt_s = (now - lastVoltageMillis) / 1000.0f;
      if (dt_s <= 0) dt_s = 1.0f;
      lastVoltageMillis = now;

      float prevFiltered = filteredVoltage;
      // EMA
      filteredVoltage = (1.0f - VOLTAGE_EMA_ALPHA) * filteredVoltage + VOLTAGE_EMA_ALPHA * pzemVoltage;

      // slope V/s and dv per sample
      float dv = filteredVoltage - prevFiltered;
      float slope = dv / dt_s;

      // adjust threshold relative to sample interval
      float sampleThreshold = SLOPE_THRESHOLD_V_PER_S * dt_s; // V per sample

      // detect rest / charging / discharging using dv vs sampleThreshold
      if (fabs(dv) < sampleThreshold) {
        if (!inRestVoltage) {
          inRestVoltage = true;
          restStartMillisVoltage = now;
        } else {
          if ((now - restStartMillisVoltage) >= REST_TIME_MS_V) {
            chargeState = CS_REST;
          }
        }
      } else {
        inRestVoltage = false;
        if (slope > SLOPE_THRESHOLD_V_PER_S) chargeState = CS_CHARGING;
        else if (slope < -SLOPE_THRESHOLD_V_PER_S) chargeState = CS_DISCHARGING;
        else chargeState = CS_UNKNOWN;
      }

      float socFromV = getSOCfromVoltage(filteredVoltage);
      float alpha = ALPHA_UNKNOWN;
      if (chargeState == CS_REST) alpha = ALPHA_REST;
      else if (chargeState == CS_CHARGING) alpha = ALPHA_CHARGING;
      else if (chargeState == CS_DISCHARGING) alpha = ALPHA_DISCHARGING;

      batterySOC = (1.0f - alpha) * batterySOC + alpha * socFromV;
      if (batterySOC < 0.0f) batterySOC = 0.0f;
      if (batterySOC > 100.0f) batterySOC = 100.0f;

      Serial.printf("Vraw=%.3f Vf=%.3f dv=%.4f slope=%.6f state=%d socV=%.1f SOC=%.1f\n",
                    pzemVoltage, filteredVoltage, dv, slope, (int)chargeState, socFromV, batterySOC);
    }

    // Time string
    char waktuStr[32];
    getTimeString(waktuStr, sizeof(waktuStr));
    char tanggal[16] = "unknown";
    char jam[16] = "unknown";

    if (strcmp(waktuStr, "Gagal ambil waktu") != 0) {
      if (strlen(waktuStr) >= 19) {
        // format "DD/MM/YYYY HH:MM:SS"
        strncpy(tanggal, waktuStr, 10);
        tanggal[10] = '\0';
        strncpy(jam, waktuStr + 11, 8);
        jam[8] = '\0';
      } else {
        strncpy(tanggal, waktuStr, sizeof(tanggal)-1);
        tanggal[sizeof(tanggal)-1] = '\0';
        jam[0] = '\0';
      }
    }

    Serial.println("================================");
    Serial.print("📅 "); Serial.println(waktuStr);
    Serial.printf("🌡 Suhu       : %.2f °C\n", suhu);
    Serial.printf("💧 Kelembapan : %.2f %%\n", kelembapan);
    Serial.printf("⏲ Tekanan    : %.2f hPa\n", tekanan);
    Serial.printf("🧪 Tekanan Wishner: %.3f bar\n", wishnerPressure);
    Serial.printf("🔌 PZEM V: %.2f V | I: %.2f A | P: %.1f W | E: %.0f Wh | SOC: %.0f%%\n", pzemVoltage, pzemCurrent, pzemPower, pzemEnergy, batterySOC);

    // -------------------------
    // PREDIKSI VELOCITY (ditampilkan di Serial Monitor)
    // -------------------------
    // Menggunakan pendekatan nomogram SDR17 (air)
// Input: di (mm) dan dp (mbar/m). Tekanan absolut (bar) tidak bisa langsung dipakai.
    // Mapping pressure -> dp/m (estimasi dinamis)
    // - Di lapangan kamu: valve ditutup -> pressure turun sampai 0, valve dibuka -> pressure naik.
    // - P_OPT = pressure "optimal" (di atas ini kita anggap dp/m sudah maksimum)
    // - P_MAX = batas atas safety (clamp)
    const float PRESSURE_OPT_BAR = 4.0f;   // bar (kondisi optimal)
    const float PRESSURE_MAX_BAR = 5.0f;   // bar (maksimum mutlak, clamp)

    // dp/m maksimum saat kondisi optimal (P = PRESSURE_OPT_BAR).
    // Default diambil dari asumsi total drop 0.1 bar pada 3.25 m -> ~30.8 mbar/m.
    // Kalau velocity terasa kekecilan/kebesaran, yang paling gampang dituning adalah angka ini.
    const float DPM_AT_OPT = ASSUMED_DP_MBAR_PER_M; // mbar/m

    // Bentuk kurva: gamma=1 linear. <1 bikin velocity lebih cepat naik di pressure rendah.
    const float PRESSURE_GAMMA = 1.0f;

    float P = wishnerPressure;
    if (P < 0.0f) P = 0.0f;
    if (P > PRESSURE_MAX_BAR) P = PRESSURE_MAX_BAR;

    float ratioP;
    if (P >= PRESSURE_OPT_BAR) {
      ratioP = 1.0f;
    } else {
      ratioP = powf(P / PRESSURE_OPT_BAR, PRESSURE_GAMMA); // 0..1
    }

    float dp_mbar_per_m = DPM_AT_OPT * ratioP; // dp/m diproposionalkan terhadap pressure
    velocity = predictVelocityNomogram(pipeDi_mm, dp_mbar_per_m);

    // ===== PATCH V15: clamp velocity =====
    if (velocity < VELOCITY_MIN) velocity = VELOCITY_MIN;
    if (velocity > VELOCITY_MAX) velocity = VELOCITY_MAX;

    Serial.printf("➡ Prediksi Velocity (di=%.1f mm, dp=%.2f mbar/m) = %.3f m/s\n",
                  pipeDi_mm, dp_mbar_per_m, velocity);

    // Build telemetry JSON payload for ThingsBoard with requested fields + batteryVoltage + battery_soc + velocity
    int systemStatus = digitalRead(RELAY_SYS_PIN) ? 1 : 0;
    int chargingStatus = relayChargeState ? 1 : 0;

    // Build JSON payload first, then compute estimated bytes and update totals
    // Increase buffer to safely include two extra numeric fields + velocity
    // ===== PATCH V15: enlarge JSON buffer =====
    char jsonPayload[768];
    int len = snprintf(jsonPayload, sizeof(jsonPayload),
                       "{\"temperature\":%.2f,\"humidity\":%.2f,\"pressure\":%.2f,"
                       "\"wishner\":%.3f,\"system\":%d,\"charging\":%d,\"batteryVoltage\":%.2f,\"battery_soc\":%.0f,\"velocity\":%.3f",
                       suhu, kelembapan, tekanan, wishnerPressure,
                       systemStatus, chargingStatus, pzemVoltage, batterySOC, velocity);
    if (len < 0 || len >= (int)sizeof(jsonPayload)) {
      Serial.println("⚠ JSON payload base truncated — consider increasing buffer.");
      // continue anyway
    }

    // Estimate bytes for this cycle (approx) based on payload length and overheadFactor
    int payloadSize = (int)strlen(jsonPayload);
    int estimatedTotal = payloadSize * overheadFactor;
    totalDataByte += (unsigned long long)estimatedTotal;

    // Append bytes info to JSON payload
    int len2 = snprintf(jsonPayload + strlen(jsonPayload), sizeof(jsonPayload) - strlen(jsonPayload),
                        ",\"bytes_cycle\":%d,\"bytes_total\":%llu}", estimatedTotal, (unsigned long long)totalDataByte);
    if (len2 < 0 || (size_t)len2 >= sizeof(jsonPayload) - strlen(jsonPayload)) {
      Serial.println("⚠ JSON payload final truncated — consider increasing buffer.");
    }

    // ===== PATCH V15: buffer CSV row (replace direct appendCSVRow) =====
    CsvRowBuffer r;
    strcpy(r.tanggal, tanggal);
    strcpy(r.waktu, jam);
    r.waterPressure = wishnerPressure;
    r.battV = pzemVoltage;
    r.battPct = batterySOC;
    r.temperature = suhu;
    r.humidity = kelembapan;
    r.airPressure = tekanan;
    r.bytesCycle = (unsigned long)estimatedTotal;
    r.bytesTotal = (unsigned long long)totalDataByte;

    if (csvBufCount == 0) lastCsvFlushMillis = millis();
    csvBuf[csvBufCount++] = r;

    // Flush ke SD jika buffer penuh
    if (csvBufCount >= CSV_BATCH_SIZE) {
      for (int i = 0; i < csvBufCount; i++) {
        bool okAppend = appendCSVRow(
          csvBuf[i].tanggal,
          csvBuf[i].waktu,
          csvBuf[i].waterPressure,
          csvBuf[i].battV,
          csvBuf[i].battPct,
          csvBuf[i].temperature,
          csvBuf[i].humidity,
          csvBuf[i].airPressure,
          csvBuf[i].bytesCycle,
          csvBuf[i].bytesTotal
        );

        if (!okAppend) {
          Serial.println("⚠ Failed append buffered CSV row");
          break; // stop flush jika error SD
        }
      }
      csvBufCount = 0;
    }

    // Publish telemetry (includes bytes_cycle and bytes_total fields and velocity)
    bool okTelemetry = publishOrQueue(topic_telemetry, jsonPayload);

    if (okTelemetry) {
      Serial.println("✅ Telemetry published to ThingsBoard.");
    } else {
      Serial.println("⚠ Telemetry queued to outbox.");
    }

    Serial.println("------------------------------");
    Serial.print("📦 Estimasi total data terkirim (per siklus): ");
    Serial.print(estimatedTotal);
    Serial.println(" byte");
    Serial.print("📊 Total kumulatif (sejak mulai): ");
    Serial.print(totalDataByte);
    Serial.print(" byte (≈ ");
    Serial.print(totalDataByte / 1024.0, 3);
    Serial.println(" kB)");
    Serial.println("------------------------------\n");

    // Check SD capacity and trim oldest CSV files if needed
    checkAndTrimDataFileIfNeeded();

  }

  // OLED cycle: update every OLED_CYCLE_INTERVAL independent of sampling
  if (millis() - lastOLEDCycle >= OLED_CYCLE_INTERVAL) {
    lastOLEDCycle = millis();
    // advance page
    oledPage = (oledPage + 1) % 7; // 7 pages: 0..6 (added Velocity page)
    updateOLEDDisplay();
  }


  // Timed flush CSV buffer (agar data tidak terlalu lama tertahan)
  if (csvBufCount > 0 && (millis() - lastCsvFlushMillis) >= CSV_FLUSH_INTERVAL_MS) {
    for (int i = 0; i < csvBufCount; i++) {
      bool okAppend = appendCSVRow(
        csvBuf[i].tanggal,
        csvBuf[i].waktu,
        csvBuf[i].waterPressure,
        csvBuf[i].battV,
        csvBuf[i].battPct,
        csvBuf[i].temperature,
        csvBuf[i].humidity,
        csvBuf[i].airPressure,
        csvBuf[i].bytesCycle,
        csvBuf[i].bytesTotal
      );
      if (!okAppend) {
        Serial.println("⚠ Failed append timed CSV flush");
        break;
      }
    }
    csvBufCount = 0;
    lastCsvFlushMillis = millis();
  }
  // periodic tasks:
  static unsigned long lastOfflineWarning = 0;
  if (!modemConnected && (millis() - lastOfflineWarning > 5UL * 60UL * 1000UL)) {
    lastOfflineWarning = millis();
    Serial.println("⚠ Device offline for extended time; monitoring...");
  }

  // Kick watchdog again near end of loop to reduce chance of timeout during long ops
  watchdogKick();
  watchdogCheck();

  delay(10);
}