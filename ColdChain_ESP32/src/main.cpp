
#include <WiFi.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <ArduinoJson.h>
#include <math.h>
#include <esp_system.h>

#define DHTPIN 4
#define DHTTYPE DHT22

#define HEATER_LED 16
#define COOLER_LED 17

const char* SSID = "Wokwi-GUEST";
const char* PASSWORD = "";

const char* BROKER = "host.wokwi.internal";
const int MQTT_PORT = 1883;

const char* TEMP_TOPIC =
    "coldchain/lab-7b39f2/zoneA/S1/temperature";

const char* STATUS_TOPIC =
    "coldchain/lab-7b39f2/zoneA/S1/status";

const char* COMMAND_TOPIC =
    "coldchain/lab-7b39f2/zoneA/S1/control/command";

const char* STATE_TOPIC =
    "coldchain/lab-7b39f2/zoneA/S1/control/state";

DHT dht(DHTPIN, DHTTYPE);

WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);

const float SAFE_MIN = -25.0f;
const float SAFE_MAX = -15.0f;

const float COOLING_RATE = 0.80f;
const float HEATING_RATE = 0.65f;
const float DRIFT_FACTOR = 0.0015f;
const float HYSTERESIS = 0.25f;

float freezerTemp = -18.0f;
float targetTemp = -20.0f;

float ambientTemp = 25.0f;
float humidity = 50.0f;

String controlMode = "auto";
String actuator = "off";

String pendingRequestId = "";
bool pendingAck = false;
bool pendingAccepted = false;

String lastAcceptedRequestId = "";
unsigned long lastAcceptedTime = 0;

unsigned long lastSensorRead = 0;
unsigned long lastControlUpdate = 0;
unsigned long lastPublish = 0;
unsigned long lastMQTTRetry = 0;
unsigned long lastWiFiRetry = 0;

void readSensor();
void updateActuator();
void regulateTemperature();
void publishTemperature();
void connectMQTT();

void publishState(
    const String& requestId = "",
    bool accepted = true
);

void queueAcknowledgement(
    const String& requestId,
    bool accepted
);

void onMessage(
    char* topic,
    byte* payload,
    unsigned int length
);


void readSensor() {

    float rawTemp = dht.readTemperature();
    float rawHum = dht.readHumidity();

    if (isfinite(rawTemp)) {
        ambientTemp = rawTemp;
    }

    if (isfinite(rawHum)) {
        humidity = rawHum;
    }

    if (!isfinite(rawTemp) || !isfinite(rawHum)) {
        Serial.println("DHT22 reading failed");
        return;
    }

    Serial.print("DHT22 Ambient: ");
    Serial.print(ambientTemp, 1);
    Serial.print(" C | Humidity: ");
    Serial.print(humidity, 1);
    Serial.println("%");
}


void updateActuator() {

    if (controlMode == "auto") {

        if (freezerTemp > targetTemp + HYSTERESIS) {
            actuator = "cool";
        }

        else if (freezerTemp < targetTemp - HYSTERESIS) {
            actuator = "heat";
        }

        else {
            actuator = "off";
        }
    }

    else if (controlMode == "cool") {

        if (freezerTemp > targetTemp + 0.05f) {
            actuator = "cool";
        }

        else {
            actuator = "off";
        }
    }

    else if (controlMode == "heat") {

        if (freezerTemp < targetTemp - 0.05f) {
            actuator = "heat";
        }

        else {
            actuator = "off";
        }
    }

    else {
        actuator = "off";
    }

    digitalWrite(
        HEATER_LED,
        actuator == "heat" ? HIGH : LOW
    );

    digitalWrite(
        COOLER_LED,
        actuator == "cool" ? HIGH : LOW
    );
}


void regulateTemperature() {

    updateActuator();

    float naturalDrift =
        DRIFT_FACTOR * (ambientTemp - freezerTemp);

    if (actuator == "cool") {

        float nextTemp =
            freezerTemp - COOLING_RATE + naturalDrift;

        freezerTemp = max(targetTemp, nextTemp);
    }

    else if (actuator == "heat") {

        float nextTemp =
            freezerTemp + HEATING_RATE + naturalDrift;

        freezerTemp = min(targetTemp, nextTemp);
    }

    else {

        freezerTemp += naturalDrift;
    }

    updateActuator();

    Serial.println();
    Serial.println("========== S1 TEMPERATURE ==========");

    Serial.print("Current: ");
    Serial.print(freezerTemp, 2);
    Serial.println(" C");

    Serial.print("Target: ");
    Serial.print(targetTemp, 2);
    Serial.println(" C");

    Serial.print("Mode: ");
    Serial.println(controlMode);

    Serial.print("Actuator: ");
    Serial.println(actuator);

    if (
        freezerTemp < SAFE_MIN ||
        freezerTemp > SAFE_MAX
    ) {
        Serial.println("Status: TEMPERATURE BREACH");
    }

    else {
        Serial.println("Status: SAFE");
    }

    Serial.println("====================================");
}


void publishState(
    const String& requestId,
    bool accepted
) {

    if (!mqtt.connected()) {
        Serial.println("STATE FAILED: MQTT disconnected");
        return;
    }

    StaticJsonDocument<512> doc;

    doc["node"] = "S1";
    doc["controller"] = true;
    doc["mode"] = controlMode;
    doc["target"] = targetTemp;
    doc["actuator"] = actuator;
    doc["ambient"] = ambientTemp;
    doc["ok"] = accepted;

    if (requestId.length() > 0) {
        doc["request_id"] = requestId;
    }

    char buffer[512];

    size_t bytes = serializeJson(
        doc,
        buffer,
        sizeof(buffer)
    );

    if (bytes == 0 || bytes >= sizeof(buffer)) {
        Serial.println("STATE JSON BUFFER ERROR");
        return;
    }

    bool sent = mqtt.publish(
        STATE_TOPIC,
        buffer
    );

    if (requestId.length() > 0) {

        Serial.println();
        Serial.println("===== ESP32 ACKNOWLEDGEMENT =====");

        Serial.print("Topic: ");
        Serial.println(STATE_TOPIC);

        Serial.print("Request ID: ");
        Serial.println(requestId);

        Serial.print("Payload: ");
        Serial.println(buffer);

        Serial.print("Publish result: ");
        Serial.println(
            sent ? "SENT" : "FAILED"
        );

        Serial.println("================================");
    }
}


void publishTemperature() {

    if (!mqtt.connected()) {
        return;
    }

    StaticJsonDocument<512> doc;

    doc["node"] = "S1";
    doc["zone"] = "zoneA";

    doc["temp"] =
        roundf(freezerTemp * 10.0f) / 10.0f;

    doc["hum"] =
        roundf(humidity * 10.0f) / 10.0f;

    doc["ambient"] = ambientTemp;
    doc["target"] = targetTemp;

    doc["mode"] = controlMode;
    doc["actuator"] = actuator;
    doc["controller"] = true;

    if (
        lastAcceptedRequestId.length() > 0 &&
        millis() - lastAcceptedTime < 30000UL
    ) {

        doc["request_id"] = lastAcceptedRequestId;
        doc["ok"] = true;
    }

    char buffer[512];

    size_t bytes = serializeJson(
        doc,
        buffer,
        sizeof(buffer)
    );

    if (bytes == 0 || bytes >= sizeof(buffer)) {
        Serial.println("TEMPERATURE JSON BUFFER ERROR");
        return;
    }

    bool sent = mqtt.publish(
        TEMP_TOPIC,
        buffer
    );

    Serial.println();
    Serial.print("Temperature MQTT: ");
    Serial.println(
        sent ? "SENT" : "FAILED"
    );

    Serial.print("Topic: ");
    Serial.println(TEMP_TOPIC);

    Serial.print("Data: ");
    Serial.println(buffer);

    publishState();
}


void queueAcknowledgement(
    const String& requestId,
    bool accepted
) {

    pendingRequestId = requestId;
    pendingAccepted = accepted;
    pendingAck = true;
}


void onMessage(
    char* topic,
    byte* payload,
    unsigned int length
) {

    Serial.println();
    Serial.println("===== MQTT MESSAGE RECEIVED =====");

    Serial.print("Topic: ");
    Serial.println(topic);

    Serial.print("Payload: ");

    for (unsigned int i = 0; i < length; i++) {
        Serial.print((char)payload[i]);
    }

    Serial.println();
    Serial.println("===============================");

    if (strcmp(topic, COMMAND_TOPIC) != 0) {

        Serial.println("Ignoring unrelated MQTT topic");
        return;
    }

    StaticJsonDocument<512> doc;

    DeserializationError error = deserializeJson(
        doc,
        payload,
        length
    );

    if (error) {

        Serial.print("Invalid JSON: ");
        Serial.println(error.c_str());
        return;
    }

    String requestId =
        doc["request_id"] | "";

    const char* node =
        doc["node"] | "S1";

    String requestedMode =
        doc["mode"] | "";

    Serial.print("Request ID: ");
    Serial.println(requestId);

    if (strcmp(node, "S1") != 0) {

        Serial.println("Command for another sensor");
        return;
    }

    if (doc["target"].isNull()) {

        Serial.println("ERROR: Missing target");

        queueAcknowledgement(
            requestId,
            false
        );

        return;
    }

    float requestedTarget =
        doc["target"].as<float>();

    bool validMode =
        requestedMode == "auto" ||
        requestedMode == "cool" ||
        requestedMode == "heat" ||
        requestedMode == "off";

    bool validTarget =
        isfinite(requestedTarget) &&
        requestedTarget >= SAFE_MIN &&
        requestedTarget <= SAFE_MAX;

    if (!validMode || !validTarget) {

        Serial.println("ERROR: Invalid command");

        queueAcknowledgement(
            requestId,
            false
        );

        return;
    }

    controlMode = requestedMode;
    targetTemp = requestedTarget;

    lastAcceptedRequestId = requestId;
    lastAcceptedTime = millis();

    updateActuator();

    Serial.println();
    Serial.println("===== COMMAND ACCEPTED =====");

    Serial.print("Mode: ");
    Serial.println(controlMode);

    Serial.print("Target: ");
    Serial.print(targetTemp, 2);
    Serial.println(" C");

    Serial.print("Actuator: ");
    Serial.println(actuator);

    Serial.print("Request ID: ");
    Serial.println(requestId);

    Serial.println("============================");

    queueAcknowledgement(
        requestId,
        true
    );
}


void connectMQTT() {

    if (mqtt.connected()) {
        return;
    }

    if (WiFi.status() != WL_CONNECTED) {
        return;
    }

    String clientId =
        "ESP32_S1_" +
        String(
            (uint32_t)ESP.getEfuseMac(),
            HEX
        ) +
        "_" +
        String(esp_random(), HEX);

    Serial.println();
    Serial.println("Connecting to HiveMQ...");

    bool connected = mqtt.connect(
        clientId.c_str(),
        STATUS_TOPIC,
        1,
        true,
        "offline"
    );

    if (!connected) {

        Serial.print("MQTT connection failed: ");
        Serial.println(mqtt.state());

        return;
    }

    Serial.println("MQTT CONNECTED");

    bool subscribed = mqtt.subscribe(
        COMMAND_TOPIC,
        1
    );

    Serial.print("Command subscription: ");

    if (subscribed) {
        Serial.println("SENT");
    }

    else {

        Serial.println("FAILED");
        mqtt.disconnect();
        return;
    }

    Serial.print("Listening on: ");
    Serial.println(COMMAND_TOPIC);

    bool onlineSent = mqtt.publish(
        STATUS_TOPIC,
        "online",
        true
    );

    Serial.print("Online status: ");
    Serial.println(
        onlineSent ? "SENT" : "FAILED"
    );

    updateActuator();

    publishState();

    Serial.println();
    Serial.println("==================================");
    Serial.println("S1 CONTROLLER READY");
    Serial.println("Waiting for dashboard commands...");
    Serial.println("==================================");
}


void setup() {

    Serial.begin(115200);

    dht.begin();

    pinMode(HEATER_LED, OUTPUT);
    pinMode(COOLER_LED, OUTPUT);

    digitalWrite(HEATER_LED, LOW);
    digitalWrite(COOLER_LED, LOW);

    Serial.println();
    Serial.println("=====================================");
    Serial.println("COLD CHAIN TEMPERATURE CONTROL SYSTEM");
    Serial.println("=====================================");
    Serial.println("Sensor: S1");
    Serial.println("Zone: A - Freezer");
    Serial.println("Safe Range: -25 C to -15 C");
    Serial.println("Initial Target: -20 C");
    Serial.println("MQTT: Isolated Wokwi S1 Topics");
    Serial.println();

    WiFi.mode(WIFI_STA);

    WiFi.begin(
        SSID,
        PASSWORD
    );

    Serial.print("Connecting to WiFi");

    while (WiFi.status() != WL_CONNECTED) {

        delay(500);
        Serial.print(".");
    }

    Serial.println();
    Serial.println("WiFi Connected");

    Serial.print("ESP32 IP: ");
    Serial.println(WiFi.localIP());

    mqtt.setServer(
        BROKER,
        MQTT_PORT
    );

    bool bufferReady = mqtt.setBufferSize(1024);

    Serial.print("MQTT buffer allocation: ");
    Serial.println(
        bufferReady ? "SUCCESS" : "FAILED"
    );

    Serial.print("MQTT buffer size: ");
    Serial.println(mqtt.getBufferSize());

    mqtt.setKeepAlive(30);
    mqtt.setSocketTimeout(5);

    mqtt.setCallback(onMessage);

    readSensor();

    updateActuator();

    connectMQTT();

    unsigned long now = millis();

    lastSensorRead = now;
    lastControlUpdate = now;
    lastPublish = now;
    lastMQTTRetry = now;
    lastWiFiRetry = now;

    Serial.println();
    Serial.println("Temperature Simulation Started");
}


void loop() {

    unsigned long now = millis();

    if (WiFi.status() != WL_CONNECTED) {

        if (now - lastWiFiRetry >= 5000) {

            Serial.println("WiFi reconnecting...");

            WiFi.reconnect();

            lastWiFiRetry = now;
        }

        delay(50);
        return;
    }

    if (!mqtt.connected()) {

        if (now - lastMQTTRetry >= 3000) {

            connectMQTT();

            lastMQTTRetry = now;
        }
    }

    if (mqtt.connected()) {

        mqtt.loop();

        if (pendingAck) {

            String requestId = pendingRequestId;
            bool accepted = pendingAccepted;

            pendingAck = false;
            pendingRequestId = "";

            publishState(
                requestId,
                accepted
            );
        }
    }

    if (now - lastSensorRead >= 2500) {

        readSensor();

        lastSensorRead = now;
    }

    if (now - lastControlUpdate >= 1000) {

        regulateTemperature();

        lastControlUpdate = now;
    }

    if (now - lastPublish >= 2000) {

        publishTemperature();

        lastPublish = now;
    }

    delay(20);
}
