
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
#define RELAY_SYS_PIN 4
#define RELAY_CHARGE_PIN 26

// RS485 control
#define EN_RS485 32

#define SerialAT Serial1
TinyGsm modem(SerialAT);
TinyGsmClient gsmClient(modem);
PubSubClient mqttClient(gsmClient);

// Modbus (PZEM-017)
ModbusMaster node;
#define SLAVE_ADDR 0x01
void preTransmission() { digitalWrite(EN_RS485, HIGH); }
void postTransmission() { digitalWrite(EN_RS485, LOW); }

// ============ SD CARD PATHS (NEW) ============
#define SD_CS 5
#define DATA_FOLDER "/data"
#define OUTBOX_FILE "/data/outbox.csv"
#define OUTBOX_IDX_FILE "/data/outbox.idx"
#define DATA_FILE_PREFIX "data_"
#define DATA_FILE_SUFFIX ".csv"

// ============ FORMAT TABLE SELECTION ============
// Pilih format: 1 = ASCII Table (rapi dengan border), 2 = TSV (tab-separated)
#define TABLE_FORMAT 1  // 1 atau 2

// Function to build full data file path
String getDataFilePath(const char* dateYYYYMMDD) {
  return String(DATA_FOLDER) + "/" + String(DATA_FILE_PREFIX) + String(dateYYYYMMDD) + String(DATA_FILE_SUFFIX);
}

// Function to ensure data folder exists
bool ensureDataFolder() {
  if (!SD.exists(DATA_FOLDER)) {
    Serial.print("📁 Creating folder: ");
    Serial.println(DATA_FOLDER);
    if (!SD.mkdir(DATA_FOLDER)) {
      Serial.println("❌ Failed to create data folder!");
      return false;
    }
    Serial.println("✅ Data folder created successfully.");
  } else {
    Serial.println("✅ Data folder already exists.");
  }
  return true;
}

// Function to ensure outbox file exists in data folder
bool ensureOutboxFile() {
  if (!SD.exists(OUTBOX_FILE)) {
    Serial.print("📄 Creating outbox file: ");
    Serial.println(OUTBOX_FILE);
    File f = SD.open(OUTBOX_FILE, FILE_WRITE);
    if (!f) {
      Serial.println("❌ Failed to create outbox file!");
      return false;
    }
    f.close();
    Serial.println("✅ Outbox file created.");
  }
  return true;
}

// ============ FORMAT TABLE FUNCTIONS ============

// Fungsi untuk menulis header tabel ASCII
void writeTableHeaderASCII(File& f) {
  f.println("╔════════════════╦══════════════╦═════════════╦═════════╦═════════════╦══════════╦═════════╦═════════╦══════════╦════════════════╦═════════════╦═══════════════╗");
  f.println("║    Tanggal     ║     Waktu    ║ Pressure(b) ║Flow(L/s)║Velocity(m/s)║BattV(V) ║BattSOC(%)║Temp(°C)║Humidity(%)║AirPressure(hPa)║BytesCycle   ║BytesTotal     ║");
  f.println("╠════════════════╬══════════════╬═════════════╬═════════╬═════════════╬══════════╬═════════╬═════════╬══════════╬════════════════╬═════════════╬═══════════════╣");
}

// Fungsi untuk menulis header tabel TSV (Tab-Separated)
void writeTableHeaderTSV(File& f) {
  f.println("Tanggal\t\tWaktu\t\tPressure\tFlow\t\tVelocity\tBatt V\t\tBatt %\t\tTemp\t\tHumidity\tAir Pressure\tBytesCycle\tBytesTotal");
  f.println("═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════");
}

// Fungsi untuk menulis baris data ASCII dengan format tabel
void writeTableRowASCII(File& f, const char* tanggal, const char* waktu,
                        float pressureBar, float flowLps, float velocityMs,
                        float battV, float battPct,
                        float temperature, float humidity, float airPressure,
                        unsigned long bytesCycle, unsigned long long bytesTotal) {
  char rowbuf[512];
  snprintf(rowbuf, sizeof(rowbuf),
           "║ %-14s║ %-12s║ %11.3f║ %7.3f║ %11.3f║ %8.2f║ %7.1f║ %7.2f║ %8.2f║ %14.2f║ %11lu║ %13llu║\n",
           tanggal, waktu,
           pressureBar, flowLps, velocityMs,
           battV, battPct,
           temperature, humidity, airPressure,
           bytesCycle, bytesTotal);
  f.print(rowbuf);
}

// Fungsi untuk menulis baris data TSV (Tab-Separated)
void writeTableRowTSV(File& f, const char* tanggal, const char* waktu,
                       float pressureBar, float flowLps, float velocityMs,
                       float battV, float battPct,
                       float temperature, float humidity, float airPressure,
                       unsigned long bytesCycle, unsigned long long bytesTotal) {
  char rowbuf[512];
  snprintf(rowbuf, sizeof(rowbuf),
           "%s\t\t%s\t\t%.3f\t\t%.3f\t\t%.3f\t\t%.2f\t\t%.1f\t\t%.2f\t\t%.2f\t\t%.2f\t\t%lu\t\t%llu\n",
           tanggal, waktu,
           pressureBar, flowLps, velocityMs,
           battV, battPct,
           temperature, humidity, airPressure,
           bytesCycle, bytesTotal);
  f.print(rowbuf);
}

// Fungsi untuk menulis footer tabel ASCII
void writeTableFooterASCII(File& f) {
  f.println("╚════════════════╩══════════════╩═════════════╩═════════╩═════════════╩══════════╩═════════╩═════════╩══════════╩════════════════╩═════════════╩═══════════════╝");
}

// ============ END SD CARD PATHS ============

// (COPY SEMUA CONFIGURATION DARI SEBELUMNYA - MQTT, SENSOR, RELAY, etc.)
// Untuk singkat, saya skip copy config dan langsung ke fungsi CSV penting

// ---------------- MQTT ----------------
const char* mqtt_server   = "mqtt.thingsboard.cloud";
const int   mqtt_port     = 1883;

const char* mqtt_clientId = "14ba1y7kw2p7qbbosfr2";
const char* mqttUsername  = "dnv855tqgrjga6aibudf";
const char* mqttPassword  = "dfie3pg7o18yq6q0yrug";

const char* topic_telemetry = "v1/devices/me/telemetry";

unsigned long lastMQTTAttempt = 0;
const unsigned long MQTT_RETRY_INTERVAL = 10000UL;

const char apn[]  = "internet";
const char gprsUser[] = "";
const char gprsPass[] = "";

Adafruit_BME280 bme;
Adafruit_SH1106G display = Adafruit_SH1106G(128, 64, &Wire, -1);

#define CSV_BATCH_SIZE 3

struct CsvRowBuffer {
  char tanggal[16];
  char waktu[16];
  float pressureBar;
  float flowLps;
  float velocityMs;
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
const unsigned long CSV_FLUSH_INTERVAL_MS = 60000UL;

Adafruit_ADS1115 ads;

const float R1 = 39000.0;
const float R2 = 100000.0;
const float dividerRatio = R2 / (R1 + R2);

const float SENSOR_VMIN = 0.5;
const float SENSOR_VMAX = 4.5;
const float PRESSURE_MIN = 0.0;
const float PRESSURE_MAX = 6.0;

float pressureOffset = 0.00;

bool manualMode = false;
float wishnerPressure = 0.0;

const char* ntpServer = "pool.ntp.org";
const long gmtOffset_sec = 7 * 3600;
const int daylightOffset_sec = 0;

unsigned long lastTime = 0;
unsigned long timerDelay = 300000;

bool modemConnected = true;
unsigned long lastModemCheck = 0;
const unsigned long modemCheckInterval = 60000UL;
const int RECONNECT_TRIES = 3;

float suhu = 0.0, kelembapan = 0.0, tekanan = 0.0;

float pzemVoltage = 0.0, pzemCurrent = 0.0, pzemPower = 0.0, pzemEnergy = 0.0;

float batterySOC = 0.0;

float velocity = 0.0f;
float flow = 0.0f;

const float VELOCITY_MIN = 0.0f;
const float VELOCITY_MAX = 10.0f;

unsigned long lastVelocityUpdate = 0;
const unsigned long VELOCITY_UPDATE_MS = 500UL;
const bool VELOCITY_REFRESH_PRESSURE = true;

float lastDpMbarPerM = 0.0f;

unsigned long long totalDataByte = 0ULL;
const float overheadFactor = 4.0;

bool systemError = false;

unsigned long lastRecoverAttempt = 0;
const unsigned long RECOVER_INTERVAL = 5 * 60 * 1000UL;
int recoverAttempts = 0;
const int MAX_RECOVER_ATTEMPTS = 3;

const uint64_t APPROX_SD_CAPACITY_BYTES = 16ULL * 1024ULL * 1024ULL * 1024ULL;
const float SD_FULL_THRESHOLD = 0.95;

const int OUTBOX_FLUSH_BATCH = 10;

bool adsDetected = false;

float currentOffset = -0.3;
const bool PZEM_USE_HOLDING_REGISTERS = false;

const float CHARGE_ON_TH = 0.2;
const float CHARGE_OFF_TH = 0.1;
const unsigned long RELAY_MIN_TOGGLE_MS = 5000;
bool relayChargeState = false;
unsigned long lastRelayChange = 0;

unsigned long lastOLEDCycle = 0;
const unsigned long OLED_CYCLE_INTERVAL = 500UL;
int oledPage = 0;
bool oledWasOn = false;
bool oledInitialized = false;

const int OLED_PAGES = 8;

const float VOLTAGE_EMA_ALPHA = 0.50f;
float filteredVoltage = 0.0f;
unsigned long lastVoltageMillis = 0;

const float SLOPE_THRESHOLD_V_PER_S = 0.0006f;
bool inRestVoltage = false;
unsigned long restStartMillisVoltage = 0;
const unsigned long REST_TIME_MS_V = 5 * 60 * 1000UL;

enum ChargeState { CS_UNKNOWN=0, CS_CHARGING=1, CS_DISCHARGING=2, CS_REST=3 };
ChargeState chargeState = CS_UNKNOWN;

const float ALPHA_REST = 0.90f;
const float ALPHA_CHARGING = 0.40f;
const float ALPHA_DISCHARGING = 0.50f;
const float ALPHA_UNKNOWN = 0.25f;

#define WDT_TIMEOUT_S 180
#define SYSTEM_ERROR_RESTART_MS (10UL * 60UL * 1000UL)
unsigned long wdtLastKick = 0;
unsigned long systemErrorSince = 0;

int cachedHour = -1;
unsigned long cachedHourMillis = 0;
const unsigned long HOUR_CACHE_MS = 5 * 60 * 1000UL;

const float NOMO_A = 0.1032658222f;
const float NOMO_B = -0.0183057214f;

float pipeDi_mm = 18.8f;

const float ASSUMED_DP_BAR_TOTAL = 0.1f;
const float ASSUMED_DISTANCE_M = 3.25f;
const float ASSUMED_DP_MBAR_PER_M = (ASSUMED_DP_BAR_TOTAL / ASSUMED_DISTANCE_M) * 1000.0f;

float predictVelocityNomogram(float di_mm, float dp_mbar_per_m) {
  if (di_mm <= 0.0f || dp_mbar_per_m <= 0.0f) return 0.0f;
  return NOMO_A * sqrtf(di_mm * dp_mbar_per_m) + NOMO_B;
}

unsigned long lastSerialBrief = 0;
const unsigned long SERIAL_BRIEF_MS = 5000UL;
unsigned long lastSerialStatus = 0;
const unsigned long SERIAL_STATUS_MS = 60000UL;

// ============ FORWARD DECLARATIONS ============
bool connectModem();
void restartModemSoft();
void modemAutoCheck();
void initModem();
void initBME();
void initSDCard();
void initOLED();
void appendFile(const char *path, const char *message);
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
bool ensureCSVExistsForDate(const char* dateYYYYMMDD);
bool appendCSVRow(const char* tanggal, const char* waktu,
                  float pressureBar, float flowLps, float velocityMs,
                  float battV, float battPct,
                  float temperature, float humidity, float airPressure,
                  unsigned long bytesCycle, unsigned long long bytesTotal);
float getSOCfromVoltage(float voltage);
void enableWatchdog();
inline void watchdogKick();
void delayWdt(unsigned long ms);
void watchdogCheck();
void checkSystemErrorRestart();
void updateVelocityFromPressure(float pressureBar);

// ============ SOC LOOKUP ============
float getSOCfromVoltage(float voltage) {
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

// ============ CSV HELPERS (MODIFIED) ============
bool ensureCSVExistsForDate(const char* dateYYYYMMDD) {
  String fname = getDataFilePath(dateYYYYMMDD);
  if (SD.exists(fname.c_str())) return true;

  Serial.print("📄 Creating CSV file: ");
  Serial.println(fname);

  File f = SD.open(fname.c_str(), FILE_WRITE);
  if (!f) {
    Serial.print("❌ Failed to create CSV file: ");
    Serial.println(fname);
    return false;
  }

  // ============ WRITE HEADER SESUAI FORMAT ============
  if (TABLE_FORMAT == 1) {
    writeTableHeaderASCII(f);
  } else if (TABLE_FORMAT == 2) {
    writeTableHeaderTSV(f);
  }

  f.close();
  Serial.println("✅ CSV header written.");
  return true;
}

bool appendCSVRow(const char* tanggal, const char* waktu,
                  float pressureBar, float flowLps, float velocityMs,
                  float battV, float battPct,
                  float temperature, float humidity, float airPressure,
                  unsigned long bytesCycle, unsigned long long bytesTotal) {
  char dateYMD[16] = {0};
  int d=0,m=0,y=0;

  if (tanggal && strlen(tanggal) >= 8) {
    if (sscanf(tanggal, "%d/%d/%d", &d, &m, &y) == 3) {
      snprintf(dateYMD, sizeof(dateYMD), "%04d%02d%02d", y, m, d);
    } else {
      struct tm ti;
      if (getLocalTime(&ti)) snprintf(dateYMD, sizeof(dateYMD), "%04d%02d%02d", ti.tm_year+1900, ti.tm_mon+1, ti.tm_mday);
      else strncpy(dateYMD, "unknown", sizeof(dateYMD)-1);
    }
  } else {
    struct tm ti;
    if (getLocalTime(&ti)) snprintf(dateYMD, sizeof(dateYMD), "%04d%02d%02d", ti.tm_year+1900, ti.tm_mon+1, ti.tm_mday);
    else strncpy(dateYMD, "unknown", sizeof(dateYMD)-1);
  }

  if (!ensureDataFolder()) {
    Serial.println("⚠ Data folder not accessible!");
    return false;
  }

  String fname = getDataFilePath(dateYMD);
  if (!SD.exists(fname.c_str())) {
    if (!ensureCSVExistsForDate(dateYMD)) return false;
  }

  File f = SD.open(fname.c_str(), FILE_APPEND);
  if (!f) {
    Serial.print("❌ Failed to open CSV file for append: ");
    Serial.println(fname);
    return false;
  }

  // ============ WRITE DATA ROW SESUAI FORMAT ============
  if (TABLE_FORMAT == 1) {
    writeTableRowASCII(f, tanggal, waktu, pressureBar, flowLps, velocityMs,
                       battV, battPct, temperature, humidity, airPressure,
                       bytesCycle, bytesTotal);
  } else if (TABLE_FORMAT == 2) {
    writeTableRowTSV(f, tanggal, waktu, pressureBar, flowLps, velocityMs,
                     battV, battPct, temperature, humidity, airPressure,
                     bytesCycle, bytesTotal);
  }

  f.close();
  return true;
}

// ============ MODEM / MQTT / SD / I2C ============
void restartModemSoft() {
  Serial.println("♻ Restarting modem (soft)...");
  if (!modem.restart()) Serial.println("⚠ modem.restart() failed");
  delayWdt(3000);
}

bool connectModem() {
  Serial.println("📶 Waiting for network (up to 60s)...");
  bool regOk = false;
  unsigned long t0 = millis();
  while (millis() - t0 < 60000UL) {
    watchdogKick();
    if (modem.waitForNetwork(15000L)) { regOk = true; break; }
    delayWdt(250);
  }
  if (!regOk) {
    Serial.println("❌ Not registered to network.");
    return false;
  }
  Serial.println("✅ Network registered.");

  Serial.println("🌐 Connecting GPRS...");
  if (!modem.gprsConnect(apn, gprsUser, gprsPass)) {
    Serial.println("❌ GPRS connect failed.");
    return false;
  }
  Serial.println("✅ GPRS connected.");
  return true;
}

void modemAutoCheck() {
  if (millis() - lastModemCheck < modemCheckInterval) return;
  lastModemCheck = millis();

  bool netOk = modem.isNetworkConnected();
  bool gprsOk = modem.isGprsConnected();
  if (netOk && gprsOk) { modemConnected = true; return; }

  Serial.println("⚠ Modem connection problem detected. Reconnecting...");
  for (int i = 0; i < RECONNECT_TRIES; ++i) {
    Serial.printf("🔁 Reconnect attempt #%d...\n", i + 1);
    if (connectModem()) { modemConnected = true; Serial.println("✅ Reconnect OK."); return; }
    delayWdt(2000);
  }

  restartModemSoft();
  if (connectModem()) { modemConnected = true; Serial.println("✅ Reconnect after restart OK."); return; }

  modemConnected = false;
  Serial.println("❌ Offline mode (SD-only).");
}

void initModem() {
  Serial.println("🔌 Init Serial for modem...");
  SerialAT.begin(115200, SERIAL_8N1, MODEM_RX, MODEM_TX);
  delayWdt(1200);

  Serial.println("🔧 Init modem...");
  modem.restart();
  while (SerialAT.available()) SerialAT.read();
  delayWdt(3000);

  Serial.print("📡 Modem: ");
  Serial.println(modem.getModemInfo());

  modemConnected = connectModem();
}

void initBME() {
  Serial.println("🌡 Init BME280...");
  bool ok = false;
  for (int i = 0; i < 3; ++i) {
    if (bme.begin(0x76)) { ok = true; break; }
    Serial.println("⚠ BME280 not found, retry...");
    delay(200);
  }
  if (!ok) {
    Serial.println("❌ BME280 init failed.");
    systemError = true;
  } else {
    Serial.println("✅ BME280 ready.");
  }
}

void initSDCard() {
  Serial.println("💾 Init SD Card...");
  if (!SD.begin(SD_CS)) { 
    Serial.println("❌ SD mount failed."); 
    systemError = true; 
    return; 
  }
  
  Serial.println("✅ SD Card mounted successfully.");

  if (!ensureDataFolder()) {
    Serial.println("❌ Failed to ensure data folder!");
    systemError = true;
    return;
  }

  if (!ensureOutboxFile()) {
    Serial.println("❌ Failed to ensure outbox file!");
    systemError = true;
    return;
  }

  Serial.println("✅ SD Card fully initialized.");
}

void initOLED() {
  Serial.println("🖥 Init OLED...");
  bool ok = false;
  for (int i = 0; i < 3; ++i) {
    if (display.begin(0x3C, true)) { ok = true; break; }
    Serial.println("⚠ OLED init failed, retry...");
    delay(200);
  }
  if (!ok) { Serial.println("❌ OLED init failed."); systemError = true; oledInitialized = false; return; }
  display.clearDisplay();
  display.display();
  oledInitialized = true;
  Serial.println("✅ OLED ready.");
}

void appendFile(const char *path, const char *message) {
  File file = SD.open(path, FILE_APPEND);
  if (!file) file = SD.open(path, FILE_WRITE);
  if (!file) {
    Serial.print("⚠ Failed to open file for append: ");
    Serial.println(path);
    return;
  }
  file.print(message);
  file.close();
}

bool connectMQTT() {
  if (mqttClient.connected()) return true;
  unsigned long now = millis();
  if (now - lastMQTTAttempt < MQTT_RETRY_INTERVAL) return false;
  lastMQTTAttempt = now;

  Serial.println("🔌 Connecting MQTT...");
  bool ok = mqttClient.connect(mqtt_clientId, mqttUsername, mqttPassword);
  if (ok) Serial.println("✅ MQTT connected.");
  else {
    Serial.print("❌ MQTT failed, rc=");
    Serial.println(mqttClient.state());
  }
  return ok;
}

bool publishOrQueue(const char* topic, const char* payload) {
  if (modem.isGprsConnected()) {
    if (!mqttClient.connected()) connectMQTT();
    if (mqttClient.connected()) {
      mqttClient.loop();
      if (mqttClient.publish(topic, payload)) return true;
      mqttClient.disconnect();
    }
  }
  
  char qline[512];
  const size_t maxPayload = 400;
  if (strlen(payload) > maxPayload) {
    char truncated[maxPayload + 1];
    strncpy(truncated, payload, maxPayload);
    truncated[maxPayload] = '\0';
    snprintf(qline, sizeof(qline), "%s|%s\n", topic, truncated);
  } else {
    snprintf(qline, sizeof(qline), "%s|%s\n", topic, payload);
  }
  
  if (!ensureDataFolder()) {
    Serial.println("⚠ Cannot ensure data folder for outbox!");
    return false;
  }
  
  appendFile(OUTBOX_FILE, qline);
  return false;
}

uint64_t readOutboxOffset() {
  if (!SD.exists(OUTBOX_IDX_FILE)) return 0ULL;
  File f = SD.open(OUTBOX_IDX_FILE, FILE_READ);
  if (!f) return 0ULL;
  String s = f.readStringUntil('\n');
  f.close();
  s.trim();
  if (s.length() == 0) return 0ULL;
  return (uint64_t)s.toInt();
}

void writeOutboxOffset(uint64_t off) {
  File f = SD.open(OUTBOX_IDX_FILE, FILE_WRITE);
  if (!f) return;
  f.seek(0);
  f.print(String((unsigned long)off));
  f.print("\n");
  f.close();
}

void flushOutbox() {
  static unsigned long lastOutboxFlushTry = 0;
  if (millis() - lastOutboxFlushTry < 30000UL) return;
  lastOutboxFlushTry = millis();

  if (!SD.exists(OUTBOX_FILE)) return;
  if (!modem.isGprsConnected()) return;
  if (!mqttClient.connected()) if (!connectMQTT()) return;

  File f = SD.open(OUTBOX_FILE, FILE_READ);
  if (!f) return;

  uint64_t off = readOutboxOffset();
  if (off > (uint64_t)f.size()) off = 0ULL;
  f.seek((uint32_t)off);

  const size_t LINE_BUF = 512;
  char lineBuf[LINE_BUF];
  int sent = 0;
  unsigned long flushStart = millis();
  const unsigned long FLUSH_BUDGET_MS = 1500UL;

  while (f.available() && sent < OUTBOX_FLUSH_BATCH) {
    watchdogKick();
    if (millis() - flushStart > FLUSH_BUDGET_MS) break;

    size_t r = f.readBytesUntil('\n', lineBuf, LINE_BUF - 1);
    if (r == 0) break;
    lineBuf[r] = '\0';
    if (r > 0 && lineBuf[r - 1] == '\r') lineBuf[r - 1] = '\0';

    char* sep = strchr(lineBuf, '|');
    if (!sep) { off = (uint64_t)f.position(); sent++; continue; }

    *sep = '\0';
    const char* topic = lineBuf;
    const char* payload = sep + 1;

    mqttClient.loop();
    if (!mqttClient.connected()) break;
    if (!mqttClient.publish(topic, payload)) break;

    off = (uint64_t)f.position();
    sent++;
  }

  f.close();
  writeOutboxOffset(off);

  File f2 = SD.open(OUTBOX_FILE, FILE_READ);
  if (f2) {
    uint32_t sz = f2.size();
    f2.close();
    if (off >= (uint64_t)sz) {
      SD.remove(OUTBOX_FILE);
      SD.remove(OUTBOX_IDX_FILE);
      Serial.println("✅ Outbox flushed.");
    }
  }
}

bool checkAndTrimDataFileIfNeeded() {
  if (APPROX_SD_CAPACITY_BYTES == 0) return true;

  struct FileInfo { String name; uint64_t size; long dateKey; bool valid; };
  const int MAX_FILES = 128;
  FileInfo files[MAX_FILES];
  int fileCount = 0;

  File root = SD.open(DATA_FOLDER);
  if (!root) return true;

  File entry;
  while ((entry = root.openNextFile())) {
    String nm = entry.name();
    if (nm.startsWith(DATA_FILE_PREFIX) && nm.endsWith(DATA_FILE_SUFFIX)) {
      if (fileCount < MAX_FILES) {
        files[fileCount].name = nm;
        files[fileCount].size = entry.size();
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

  Serial.println("⚠ SD near full, trimming old CSV files...");
  while (totalSize > threshold && fileCount > 0) {
    int oldestIdx = -1;
    long oldestKey = LONG_MAX;

    for (int i = 0; i < fileCount; ++i) {
      if (!files[i].valid) continue;
      long k = files[i].dateKey;
      if (k == 0) {
        if (oldestIdx == -1) { oldestIdx = i; oldestKey = 0; }
        else if (files[i].name < files[oldestIdx].name) oldestIdx = i;
      } else {
        if (k < oldestKey) { oldestKey = k; oldestIdx = i; }
      }
    }
    if (oldestIdx == -1) break;

    String fullPath = String(DATA_FOLDER) + "/" + files[oldestIdx].name;
    if (SD.remove(fullPath.c_str())) {
      Serial.print("🗑 Deleted: ");
      Serial.println(fullPath);
      totalSize -= files[oldestIdx].size;
    }
    files[oldestIdx].valid = false;
  }
  return true;
}

void attemptRecover() {
  if (!systemError) return;
  if (millis() - lastRecoverAttempt < RECOVER_INTERVAL) return;

  lastRecoverAttempt = millis();
  recoverAttempts++;

  Serial.printf("🔁 Attempt recovery #%d...\n", recoverAttempts);

  systemError = false;
  initBME();
  initSDCard();
  initOLED();

  if (!modem.isGprsConnected()) initModem();
  if (!adsDetected) adsDetected = initADSRobust(0x4B, 3, 300);

  if (!systemError) {
    Serial.println("✅ Recovery success.");
    digitalWrite(RELAY_SYS_PIN, HIGH);
    recoverAttempts = 0;
  } else {
    Serial.println("⚠ Recovery failed.");
  }

  if (recoverAttempts >= MAX_RECOVER_ATTEMPTS) {
    Serial.println("❌ Max recover attempts reached, restarting...");
    delayWdt(2000);
    ESP.restart();
  }
}

void i2cScan() {
  Serial.println("🔍 I2C scan:");
  bool found = false;
  for (uint8_t addr = 1; addr < 127; ++addr) {
    Wire.beginTransmission(addr);
    if (Wire.endTransmission() == 0) {
      Serial.print(" - Found 0x");
      if (addr < 16) Serial.print('0');
      Serial.println(addr, HEX);
      found = true;
    }
  }
  if (!found) Serial.println(" - No I2C devices found");
}

bool initADSRobust(uint8_t address, int retries, int delayMs) {
  Serial.print("🧪 Init ADS1115 at 0x");
  Serial.println(address, HEX);
  for (int i = 0; i < retries; ++i) {
    if (ads.begin(address)) {
      ads.setGain(GAIN_ONE);
      Serial.println("✅ ADS1115 ready.");
      return true;
    }
    Serial.println("⚠ ADS1115 not found, retry...");
    delay(delayMs);
  }
  Serial.println("❌ ADS1115 init failed.");
  return false;
}

float adsVoltageFromRaw(int16_t raw, int gainSetting) {
  float fs = 4.096f;
  switch (gainSetting) {
    case GAIN_TWOTHIRDS: fs = 6.144f; break;
    case GAIN_ONE:       fs = 4.096f; break;
    case GAIN_TWO:       fs = 2.048f; break;
    case GAIN_FOUR:      fs = 1.024f; break;
    case GAIN_EIGHT:     fs = 0.512f; break;
    case GAIN_SIXTEEN:   fs = 0.256f; break;
    default:             fs = 4.096f; break;
  }
  return (raw * fs) / 32767.0f;
}

// ============ TIME HELPERS ============
bool getTimeFromModem(char* outBuf, size_t outLen) {
  SerialAT.println("AT+CCLK?");
  unsigned long start = millis();
  String found = "";
  while (millis() - start < 3000) {
    if (SerialAT.available()) {
      String line = SerialAT.readStringUntil('\n');
      line.trim();
      if (line.length() == 0) continue;
      if (line.indexOf("+CCLK:") != -1) { found = line; break; }
      if (line.indexOf('"') != -1 && line.indexOf("OK") == -1) { found = line; break; }
    }
  }
  if (found.length() == 0) return false;

  int q1 = found.indexOf('"');
  int q2 = found.indexOf('"', q1 + 1);
  String timestr = (q1 != -1 && q2 != -1) ? found.substring(q1 + 1, q2) : found;
  timestr.trim();

  if (timestr.length() >= 17) {
    int firstSlash = timestr.indexOf('/');
    int secondSlash = timestr.indexOf('/', firstSlash + 1);
    int comma = timestr.indexOf(',');
    if (firstSlash == -1 || secondSlash == -1 || comma == -1) return false;

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
  if (getTimeFromModem(outBuf, outLen)) return true;
  strncpy(outBuf, "Gagal ambil waktu", outLen);
  outBuf[outLen - 1] = '\0';
  return false;
}

int getHourFromModem() {
  SerialAT.println("AT+CCLK?");
  unsigned long start = millis();
  String found = "";
  while (millis() - start < 1500) {
    if (SerialAT.available()) {
      String line = SerialAT.readStringUntil('\n');
      line.trim();
      if (line.length() == 0) continue;
      if (line.indexOf("+CCLK:") != -1) { found = line; break; }
      if (line.indexOf('"') != -1 && line.indexOf("OK") == -1) { found = line; break; }
    }
  }
  if (found.length() == 0) return -1;

  int q1 = found.indexOf('"');
  int q2 = found.indexOf('"', q1 + 1);
  String timestr = (q1 != -1 && q2 != -1) ? found.substring(q1 + 1, q2) : found;
  timestr.trim();

  int comma = timestr.indexOf(',');
  String timePart = (comma != -1) ? timestr.substring(comma + 1) : timestr;
  timePart.trim();

  int c1 = timePart.indexOf(':');
  if (c1 == -1) return -1;

  int hour = timePart.substring(0, c1).toInt();
  if (hour < 0 || hour > 23) return -1;
  return hour;
}

int getCurrentHour() {
  struct tm timeinfo;
  if (getLocalTime(&timeinfo)) {
    cachedHour = timeinfo.tm_hour;
    cachedHourMillis = millis();
    return cachedHour;
  }
  unsigned long now = millis();
  if ((cachedHour >= 0) && (now - cachedHourMillis <= HOUR_CACHE_MS)) return cachedHour;

  int h = getHourFromModem();
  if (h >= 0) { cachedHour = h; cachedHourMillis = now; }
  return h;
}

bool getTimeShortString(char* outBuf, size_t outLen) {
  struct tm timeinfo;
  if (getLocalTime(&timeinfo)) {
    snprintf(outBuf, outLen, "%02d:%02d:%02d", timeinfo.tm_hour, timeinfo.tm_min, timeinfo.tm_sec);
    return true;
  }
  char modBuf[64] = {0};
  if (getTimeFromModem(modBuf, sizeof(modBuf))) {
    String s = String(modBuf);
    int sp = s.indexOf(' ');
    String timePart = (sp != -1) ? s.substring(sp + 1) : s;
    timePart.trim();
    int c1 = timePart.indexOf(':');
    int c2 = timePart.indexOf(':', c1 + 1);
    if (c1 == -1) return false;
    int h = timePart.substring(0, c1).toInt();
    int m = (c2 == -1) ? timePart.substring(c1 + 1).toInt() : timePart.substring(c1 + 1, c2).toInt();
    int sec = (c2 == -1) ? 0 : timePart.substring(c2 + 1).toInt();
    if (h < 0 || h > 23) return false;
    snprintf(outBuf, outLen, "%02d:%02d:%02d", h, m, sec);
    return true;
  }
  return false;
}

const bool DEFAULT_OLED_FALLBACK = false;
bool isOLEDTimeWindow() {
  int hour = getCurrentHour();
  if (hour < 0) return DEFAULT_OLED_FALLBACK;
  return (hour >= 8 && hour < 17);
}

bool isChargeAllowed() {
  int hour = getCurrentHour();
  if (hour < 0) return true;
  return (hour >= 6 && hour < 18);
}

// ============ SENSOR READ ============
float readWishnerPressure() {
  float v_sensor;
  if (manualMode) {
    if (Serial.available()) v_sensor = Serial.parseFloat();
    else return wishnerPressure;
  } else {
    if (!adsDetected) return wishnerPressure;
    int16_t adcValue = ads.readADC_SingleEnded(0);
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

// ============ PZEM MODBUS HELPERS ============
bool baca16_retry(uint16_t reg, uint16_t &out, int retries, int waitMs) {
  for (int i = 0; i < retries; ++i) {
    uint8_t res = PZEM_USE_HOLDING_REGISTERS ? node.readHoldingRegisters(reg, 1) : node.readInputRegisters(reg, 1);
    if (res == node.ku8MBSuccess) { out = node.getResponseBuffer(0); return true; }
    delay(waitMs);
  }
  return false;
}

bool baca32_retry(uint16_t reg, uint32_t &out, int retries, int waitMs) {
  for (int i = 0; i < retries; ++i) {
    uint8_t res = PZEM_USE_HOLDING_REGISTERS ? node.readHoldingRegisters(reg, 2) : node.readInputRegisters(reg, 2);
    if (res == node.ku8MBSuccess) {
      uint32_t high = node.getResponseBuffer(0);
      uint32_t low  = node.getResponseBuffer(1);
      out = (high << 16) | low;
      return true;
    }
    delay(waitMs);
  }
  return false;
}

// ============ RELAY UPDATE ============
void updateRelayWithHysteresis(float current) {
  unsigned long now = millis();

  if (!isChargeAllowed()) {
    if (relayChargeState) {
      relayChargeState = false;
      digitalWrite(RELAY_CHARGE_PIN, LOW);
      lastRelayChange = now;
      Serial.println("⚡ Relay CHARGE OFF (schedule)");
    }
    return;
  }

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

// ============ VELOCITY/FLOW UPDATE ============
void updateVelocityFromPressure(float pressureBar) {
  const float PRESSURE_OPT_BAR = 4.0f;
  const float PRESSURE_MAX_BAR = 5.0f;
  const float DPM_AT_OPT = ASSUMED_DP_MBAR_PER_M;
  const float PRESSURE_GAMMA = 1.0f;

  float P = pressureBar;
  if (P < 0.0f) P = 0.0f;
  if (P > PRESSURE_MAX_BAR) P = PRESSURE_MAX_BAR;

  float ratioP = (P >= PRESSURE_OPT_BAR) ? 1.0f : powf(P / PRESSURE_OPT_BAR, PRESSURE_GAMMA);
  float dp_mbar_per_m = DPM_AT_OPT * ratioP;
  lastDpMbarPerM = dp_mbar_per_m;

  float v = predictVelocityNomogram(pipeDi_mm, dp_mbar_per_m);
  if (v < VELOCITY_MIN) v = VELOCITY_MIN;
  if (v > VELOCITY_MAX) v = VELOCITY_MAX;
  velocity = v;

  flow = (2.8f - velocity) * 31.4f;
  if (flow < 0.0f) flow = 0.0f;
}

// ============ OLED ============
void updateOLEDDisplay() {
  if (!oledInitialized) return;

  bool shouldOn = isOLEDTimeWindow();
  if (!shouldOn) {
    if (oledWasOn) {
      display.clearDisplay();
      display.display();
      oledWasOn = false;
    }
    return;
  }

  oledWasOn = true;
  display.clearDisplay();
  display.setTextSize(1);
  display.setTextColor(SH110X_WHITE);

  switch (oledPage) {
    case 0:
      display.setCursor(0, 0);
      display.print("PT. SCTK");
      display.setCursor(0, 20);
      display.print("Pressure Monitoring");
      display.setCursor(0, 40);
      display.print("By: Desta & Hanif");
      break;

    case 1:
      display.setTextSize(2);
      display.setCursor(0, 0);
      display.print("Pressure:");
      display.setCursor(0, 20);
      {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.3f bar", wishnerPressure);
        display.print(buf);
      }
      display.setTextSize(1);
      break;

    case 2:
      display.setTextSize(2);
      display.setCursor(0, 0);
      display.print("Velocity:");
      display.setCursor(0, 20);
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
      display.print("Flow:");
      display.setCursor(0, 20);
      {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.3f L/s", flow);
        display.print(buf);
      }
      display.setTextSize(1);
      break;

    case 4:
      display.setTextSize(2);
      display.setCursor(0, 0);
      display.print("Temp:");
      display.setCursor(0, 20);
      {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.2f C", suhu);
        display.print(buf);
      }
      display.setTextSize(1);
      break;

    case 5:
      display.setTextSize(2);
      display.setCursor(0, 0);
      display.print("Hum:");
      display.setCursor(0, 20);
      {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.2f %%", kelembapan);
        display.print(buf);
      }
      display.setTextSize(1);
      break;

    case 6:
      display.setTextSize(2);
      display.setCursor(0, 0);
      display.print("V Batt:");
      display.setCursor(0, 20);
      {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.2f V", pzemVoltage);
        display.print(buf);
      }
      display.setTextSize(1);
      break;

    case 7:
      display.setTextSize(2);
      display.setCursor(0, 0);
      display.print("Batt:");
      display.setCursor(0, 20);
      {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.0f %%", batterySOC);
        display.print(buf);
      }
      display.setTextSize(1);
      break;
  }

  char shortTime[16] = {0};
  display.setCursor(0, 56);
  display.setTextSize(1);
  if (getTimeShortString(shortTime, sizeof(shortTime))) display.print(shortTime);
  else display.print("---:--:--");

  display.display();
}

// ============ WATCHDOG ============
void enableWatchdog() { wdtLastKick = millis(); }
inline void watchdogKick() { wdtLastKick = millis(); }

void delayWdt(unsigned long ms) {
  unsigned long start = millis();
  while (millis() - start < ms) {
    watchdogKick();
    mqttClient.loop();
    delay(10);
  }
}

void watchdogCheck() {
  if (wdtLastKick == 0) return;
  unsigned long now = millis();
  if ((now - wdtLastKick) > (unsigned long)(WDT_TIMEOUT_S * 1000UL)) {
    Serial.println("❗ WDT timeout -> restart");
    delay(200);
    ESP.restart();
  }
}

void checkSystemErrorRestart() {
  if (!systemError) { systemErrorSince = 0; return; }
  if (systemErrorSince == 0) systemErrorSince = millis();
  if ((millis() - systemErrorSince) >= SYSTEM_ERROR_RESTART_MS) {
    Serial.println("❗ systemError too long -> restart");
    delay(200);
    ESP.restart();
  }
}

// ============ SETUP & LOOP ============
void setup() {
  Serial.begin(115200);
  delay(100);

  pinMode(RELAY_SYS_PIN, OUTPUT);
  digitalWrite(RELAY_SYS_PIN, LOW);

  pinMode(RELAY_CHARGE_PIN, OUTPUT);
  digitalWrite(RELAY_CHARGE_PIN, LOW);

  pinMode(EN_RS485, OUTPUT);
  digitalWrite(EN_RS485, LOW);

  #if TABLE_FORMAT == 1
    Serial.println("=== FORMAT: ASCII TABLE ===");
  #else
    Serial.println("=== FORMAT: TSV (TAB-SEPARATED) ===");
  #endif

  Serial.println("=== START ESP32 SIM7600G + BME + WISHNER + PZEM + SD + OLED + TB ===");

  initModem();
  mqttClient.setServer(mqtt_server, mqtt_port);
  mqttClient.setKeepAlive(120);

  Serial2.begin(9600, SERIAL_8N1, 27, 25);
  node.begin(SLAVE_ADDR, Serial2);
  node.preTransmission(preTransmission);
  node.postTransmission(postTransmission);

  Wire.begin(21, 22);
  delay(200);
  i2cScan();

  initBME();
  initOLED();

  uint8_t detectedAddr = 0;
  for (uint8_t a = 0x48; a <= 0x4B; ++a) {
    Wire.beginTransmission(a);
    if (Wire.endTransmission() == 0) { detectedAddr = a; break; }
  }
  adsDetected = detectedAddr ? initADSRobust(detectedAddr, 5, 200) : initADSRobust(0x4B, 5, 200);

  initSDCard();

  configTime(gmtOffset_sec, daylightOffset_sec, ntpServer);
  delayWdt(2000);

  if (!systemError) {
    digitalWrite(RELAY_SYS_PIN, HIGH);
    Serial.println("✅ System OK, RELAY_SYS ON");
  }

  enableWatchdog();
  checkAndTrimDataFileIfNeeded();

  lastOLEDCycle = millis();
  oledPage = 0;

  wishnerPressure = readWishnerPressure();
  updateVelocityFromPressure(wishnerPressure);
  lastVelocityUpdate = millis();

  Serial.printf("INIT -> Pressure=%.3f bar | dp=%.2f mbar/m | Velocity=%.3f m/s | Flow=%.3f L/s\n",
                wishnerPressure, lastDpMbarPerM, velocity, flow);

  updateOLEDDisplay();
}

void loop() {
    if (!isChargeAllowed() && relayChargeState) {
    relayChargeState = false;
    digitalWrite(RELAY_CHARGE_PIN, LOW);
    lastRelayChange = millis();
    Serial.println("⚡ Relay CHARGE OFF (schedule enforce)");
  }

  watchdogKick();
  watchdogCheck();

  if (systemError) {
    digitalWrite(RELAY_SYS_PIN, LOW);
    Serial.println("❌ systemError=TRUE -> recovery mode");
    attemptRecover();
    checkSystemErrorRestart();
    delay(1000);
    return;
  }

  modemAutoCheck();
  mqttClient.loop();

  if (modem.isGprsConnected()) flushOutbox();

  // --- SERIAL status (tiap 60 detik) ---
  if (millis() - lastSerialStatus >= SERIAL_STATUS_MS) {
    lastSerialStatus = millis();
    Serial.printf("STATUS -> NET:%d GPRS:%d MQTT:%d ADS:%d SD:%d\n",
                  modem.isNetworkConnected() ? 1 : 0,
                  modem.isGprsConnected() ? 1 : 0,
                  mqttClient.connected() ? 1 : 0,
                  adsDetected ? 1 : 0,
                  SD.begin(SD_CS) ? 1 : 0);
  }

  // ALWAYS update velocity/flow periodically (responsive)
  if (millis() - lastVelocityUpdate >= VELOCITY_UPDATE_MS) {
    lastVelocityUpdate = millis();
    if (VELOCITY_REFRESH_PRESSURE) wishnerPressure = readWishnerPressure();
    updateVelocityFromPressure(wishnerPressure);

    // --- SERIAL brief (tiap 5 detik) ---
    if (millis() - lastSerialBrief >= SERIAL_BRIEF_MS) {
      lastSerialBrief = millis();
      Serial.printf("BRIEF -> Pressure=%.3f bar | dp=%.2f mbar/m | V=%.3f m/s | Flow=%.3f L/s\n",
                    wishnerPressure, lastDpMbarPerM, velocity, flow);
    }
  }

  // sensor sampling (5 menit)
  if ((millis() - lastTime) > timerDelay) {
    unsigned long now = millis();
    lastTime = now;

    // BME readings
    suhu = bme.readTemperature();
    kelembapan = bme.readHumidity();
    tekanan = bme.readPressure() / 100.0F;

    if (isnan(suhu) || isnan(kelembapan) || isnan(tekanan)) {
      Serial.println("❌ BME read invalid -> systemError");
      systemError = true;
      lastRecoverAttempt = millis();
      return;
    }

    // pressure read (if not already refreshed frequently)
    if (!VELOCITY_REFRESH_PRESSURE) wishnerPressure = readWishnerPressure();
    if (isnan(wishnerPressure)) {
      Serial.println("❌ Wishner pressure invalid -> systemError");
      systemError = true;
      lastRecoverAttempt = millis();
      return;
    }

    // ensure velocity/flow fresh before telemetry & CSV
    updateVelocityFromPressure(wishnerPressure);

    // PZEM read
    uint16_t v16;
    uint32_t v32;

    if (baca16_retry(0x0000, v16, 2, 100)) pzemVoltage = v16 / 100.0;
    else pzemVoltage = 0.0;

    if (baca16_retry(0x0001, v16, 2, 100)) pzemCurrent = v16 / 100.0;
    else pzemCurrent = 0.0;

    if (baca32_retry(0x0002, v32, 2, 100)) pzemPower = v32 / 10.0;
    else pzemPower = 0.0;

    if (baca32_retry(0x0004, v32, 2, 100)) pzemEnergy = (float)v32;
    else pzemEnergy = 0.0;

    pzemCurrent += currentOffset;
    updateRelayWithHysteresis(pzemCurrent);

    // SOC update
    if (lastVoltageMillis == 0) {
      filteredVoltage = pzemVoltage;
      lastVoltageMillis = now;
      batterySOC = getSOCfromVoltage(filteredVoltage);
    } else {
      float dt_s = (now - lastVoltageMillis) / 1000.0f;
      if (dt_s <= 0) dt_s = 1.0f;
      lastVoltageMillis = now;

      float prevFiltered = filteredVoltage;
      filteredVoltage = (1.0f - VOLTAGE_EMA_ALPHA) * filteredVoltage + VOLTAGE_EMA_ALPHA * pzemVoltage;

      float dv = filteredVoltage - prevFiltered;
      float slope = dv / dt_s;
      float sampleThreshold = SLOPE_THRESHOLD_V_PER_S * dt_s;

      if (fabs(dv) < sampleThreshold) {
        if (!inRestVoltage) { inRestVoltage = true; restStartMillisVoltage = now; }
        else if ((now - restStartMillisVoltage) >= REST_TIME_MS_V) chargeState = CS_REST;
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
    }

    // Time strings
    char waktuStr[32];
    getTimeString(waktuStr, sizeof(waktuStr));
    char tanggal[16] = "unknown";
    char jam[16] = "unknown";
    if (strcmp(waktuStr, "Gagal ambil waktu") != 0 && strlen(waktuStr) >= 19) {
      strncpy(tanggal, waktuStr, 10); tanggal[10] = '\0';
      strncpy(jam, waktuStr + 11, 8); jam[8] = '\0';
    }

    // Build telemetry JSON: include flow_lps AND velocity
    int systemStatus = digitalRead(RELAY_SYS_PIN) ? 1 : 0;
    int chargingStatus = relayChargeState ? 1 : 0;

    char jsonPayload[768];
    snprintf(jsonPayload, sizeof(jsonPayload),
             "{\"temperature\":%.2f,\"humidity\":%.2f,\"pressure\":%.2f,"
             "\"wishner\":%.3f,\"system\":%d,\"charging\":%d,"
             "\"batteryVoltage\":%.2f,\"battery_soc\":%.0f,"
             "\"flow_lps\":%.3f,\"velocity\":%.3f",
             suhu, kelembapan, tekanan,
             wishnerPressure, systemStatus, chargingStatus,
             pzemVoltage, batterySOC,
             flow, velocity);

    int payloadSize = (int)strlen(jsonPayload);
    int estimatedTotal = (int)(payloadSize * overheadFactor);
    totalDataByte += (unsigned long long)estimatedTotal;

    snprintf(jsonPayload + strlen(jsonPayload), sizeof(jsonPayload) - strlen(jsonPayload),
             ",\"bytes_cycle\":%d,\"bytes_total\":%llu}", estimatedTotal, (unsigned long long)totalDataByte);

    // Buffer CSV row
    CsvRowBuffer r;
    strcpy(r.tanggal, tanggal);
    strcpy(r.waktu, jam);
    r.pressureBar = wishnerPressure;
    r.flowLps = flow;
    r.velocityMs = velocity;
    r.battV = pzemVoltage;
    r.battPct = batterySOC;
    r.temperature = suhu;
    r.humidity = kelembapan;
    r.airPressure = tekanan;
    r.bytesCycle = (unsigned long)estimatedTotal;
    r.bytesTotal = (unsigned long long)totalDataByte;

    if (csvBufCount == 0) lastCsvFlushMillis = millis();
    csvBuf[csvBufCount++] = r;

    if (csvBufCount >= CSV_BATCH_SIZE) {
      for (int i = 0; i < csvBufCount; i++) {
        bool okAppend = appendCSVRow(
          csvBuf[i].tanggal,
          csvBuf[i].waktu,
          csvBuf[i].pressureBar,
          csvBuf[i].flowLps,
          csvBuf[i].velocityMs,
          csvBuf[i].battV,
          csvBuf[i].battPct,
          csvBuf[i].temperature,
          csvBuf[i].humidity,
          csvBuf[i].airPressure,
          csvBuf[i].bytesCycle,
          csvBuf[i].bytesTotal
        );
        if (!okAppend) { Serial.println("⚠ CSV append failed (batch)"); break; }
      }
      csvBufCount = 0;
    }

    // Publish telemetry (or queue)
    bool okPub = publishOrQueue(topic_telemetry, jsonPayload);

    // ============ FULL SERIAL REPORT ============
    Serial.println("==========================================");
    Serial.print("📅 TIME: "); Serial.println(waktuStr);
    Serial.printf("🌡 Temp: %.2f C | 💧 Hum: %.2f %% | 🧭 AirP: %.2f hPa\n", suhu, kelembapan, tekanan);
    Serial.printf("🧪 Wishner Pressure: %.3f bar\n", wishnerPressure);
    Serial.printf("➡ dp: %.2f mbar/m | Velocity: %.3f m/s | Flow: %.3f L/s\n", lastDpMbarPerM, velocity, flow);
    Serial.printf("🔌 PZEM: V=%.2f V | I=%.2f A | P=%.1f W | E=%.0f Wh\n", pzemVoltage, pzemCurrent, pzemPower, pzemEnergy);
    Serial.printf("🔋 SOC: %.0f %% | RelaySYS=%d | RelayCHG=%d\n", batterySOC, systemStatus, chargingStatus);
    Serial.printf("📦 bytes_cycle=%lu | bytes_total=%llu | publish=%s\n",
                  (unsigned long)estimatedTotal, (unsigned long long)totalDataByte,
                  okPub ? "OK" : "QUEUED");

    #if TABLE_FORMAT == 1
      Serial.println("📋 Format: ASCII TABLE");
    #else
      Serial.println("📋 Format: TSV (TAB-SEPARATED)");
    #endif

    Serial.printf("📁 CSV stored in: %s\n", getDataFilePath(tanggal).c_str());
    Serial.println("==========================================\n");
    // =================================================

    // SD trim check
    checkAndTrimDataFileIfNeeded();
  }

  // OLED cycle
  if (millis() - lastOLEDCycle >= OLED_CYCLE_INTERVAL) {
    lastOLEDCycle = millis();
    oledPage = (oledPage + 1) % OLED_PAGES;
    updateOLEDDisplay();
  }

  // Timed flush CSV buffer
  if (csvBufCount > 0 && (millis() - lastCsvFlushMillis) >= CSV_FLUSH_INTERVAL_MS) {
    for (int i = 0; i < csvBufCount; i++) {
      bool okAppend = appendCSVRow(
        csvBuf[i].tanggal,
        csvBuf[i].waktu,
        csvBuf[i].pressureBar,
        csvBuf[i].flowLps,
        csvBuf[i].velocityMs,
        csvBuf[i].battV,
        csvBuf[i].battPct,
        csvBuf[i].temperature,
        csvBuf[i].humidity,
        csvBuf[i].airPressure,
        csvBuf[i].bytesCycle,
        csvBuf[i].bytesTotal
      );
      if (!okAppend) { Serial.println("⚠ CSV append failed (timed flush)"); break; }
    }
    csvBufCount = 0;
    lastCsvFlushMillis = millis();
  }

  watchdogKick();
  watchdogCheck();
  delay(10);
}
