#!/bin/bash
# certs/generate.sh
set -e

PASSWORD="password"
VALIDITY=365
DNAME="CN=localhost,OU=kafka,O=test,L=city,ST=state,C=RU"
CLIENT_DNAME="CN=client,OU=kafka,O=test,L=city,ST=state,C=RU"

mkdir -p certs

# 1. Создание CA
openssl req -new -x509 -keyout certs/ca-key.pem -out certs/ca-cert.pem \
  -days "$VALIDITY" -passout pass:"$PASSWORD" -subj "/CN=KafkaCA"

# Функция генерации Keystore для брокера/клиента
generate_keystore() {
  local NAME=$1
  local DNAME_INPUT=$2
  local ALIAS=$3

  
  keytool -genkeypair -keystore "certs/kafka.${NAME}.keystore.jks" \
    -alias "$ALIAS" -validity "$VALIDITY" -storepass "$PASSWORD" -keypass "$PASSWORD" \
    -dname "$DNAME_INPUT" -storetype JKS -noprompt \
    -keyalg RSA -keysize 2048

  # Создаём CSR
  keytool -certreq -keystore "certs/kafka.${NAME}.keystore.jks" \
    -alias "$ALIAS" -file "certs/${NAME}.csr" -storepass "$PASSWORD"

  # Подписываем CSR нашим CA
  openssl x509 -req -CA certs/ca-cert.pem -CAkey certs/ca-key.pem \
    -in "certs/${NAME}.csr" -out "certs/${NAME}.signed" -days "$VALIDITY" \
    -CAcreateserial -passin pass:"$PASSWORD"

 
  keytool -importcert -keystore "certs/kafka.${NAME}.keystore.jks" \
    -file certs/ca-cert.pem -alias CARoot -storepass "$PASSWORD" -noprompt

  keytool -importcert -keystore "certs/kafka.${NAME}.keystore.jks" \
    -file "certs/${NAME}.signed" -alias "$ALIAS" -storepass "$PASSWORD" -noprompt
}

# 2. Keystore для брокеров
for i in 1 2 3; do
  generate_keystore "broker${i}" "$DNAME" "localhost"
done

# 3. Keystore для клиента (продюсер/консьюмер)
generate_keystore "client" "$CLIENT_DNAME" "client"

# 4. Общий Truststore (содержит только CA)
keytool -importcert -keystore certs/kafka.truststore.jks \
  -file certs/ca-cert.pem -alias CARoot -storepass "$PASSWORD" -noprompt

# 5. Файлы с паролями для Docker
echo "$PASSWORD" > certs/keystore_creds
echo "$PASSWORD" > certs/truststore_creds

chmod 600 certs/*
echo "✅ Сертификаты успешно сгенерированы в папке certs/"