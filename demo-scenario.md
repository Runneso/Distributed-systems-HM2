# Demo scenarios

---

## Сценарий 1 — follower отклоняет запись

```bash
# Запускаем 2 узла 
./node.exe -id=A -host="127.0.0.1" -port=8081
./node.exe -id=B -host="127.0.0.1" -port=8082

# Запускаем CLI
./cli.exe 
# Настраиваем кластер
addNode A 127.0.0.1 8081
addNode B 127.0.0.1 8082
setReplication <async|sync|semi-sync>
setRF 2
setLeader A
# Пробуем записать через follower
put <key> <value> --target B
# Ожидаем ошибку, что B не может принять запись, так как он follower
```

---

## Сценарий 2 — async lag + stale read

```bash
# Запускаем 2 узла 
./node.exe -id=A -host="127.0.0.1" -port=8081
./node.exe -id=B -host="127.0.0.1" -port=8082

# Запускаем CLI
./cli.exe 
# Настраиваем кластер
addNode A 127.0.0.1 8081
addNode B 127.0.0.1 8082
setReplication async
setRF 2
setLeader A
# Ставим ощутимую задержку
setReplicationDelayMs 5000 5000
# Делаем запись на leader
put <key> <value> --target A
# Сразу же делаем чтение на follower
get <key> --target B
# Ожидаем stale read (может быть empty или старое значение)
# Ждем 5 секунд
# Делаем повторное чтение на follower
get <key> --target B
# Ожидаем новое значение, которое было записано на leader
```

---

## Сценарий 3 — sync + RF

```bash
# Запускаем 3 узла 
./node.exe -id=A -host="127.0.0.1" -port=8081
./node.exe -id=B -host="127.0.0.1" -port=8082
./node.exe -id=C -host="127.0.0.1" -port=8083
# Настраиваем кластер
addNode A 127.0.0.1 8081
addNode B 127.0.0.1 8082
addNode C 127.0.0.1 8083
setReplication sync
setRF 3
setLeader A
# Делаем запись на leader
put <key> <value> --target A
# Ожидаем, что запись будет подтверждена всеми 3 узлами 
# Убиваем одного из followers (например, B)
# Делаем повторную запись на leader
put <key> <value> --target A
# Ожидаем ошибку о невозможности достичь RF=3, так как B недоступен
# Меняем RF на 2
setRF 2
# Делаем повторную запись на leader
put <key> <value> --target A
# Ожидаем успешное подтверждение, так как теперь достаточно 2 узлов 
# Восстанавливаем B
# Возвращаем RF на 3
setRF 3
# Делаем запись на leader
put <key> <value> --target A
# Ожидаем успешное подтверждение, так как все узлы доступны
```

---

## Сценарий 4 — semi-sync: быстрое OK, но догоняем в фоне

```bash
# Запускаем 3 узла 
./node.exe -id=A -host="127.0.0.1" -port=8081
./node.exe -id=B -host="127.0.0.1" -port=8082
./node.exe -id=C -host="127.0.0.1" -port=8083
# Настраиваем кластер
addNode A 127.0.0.1 8081
addNode B 127.0.0.1 8082
addNode C 127.0.0.1 8083
setReplication semi-sync
setRF 3
setSemiSyncAcks 1
setLeader A
# Ставим большой разборс задержки для узлов
setReplicationDelayMs 0 10000
# Делаем запись на leader
put <key> <value> --target A
# Ожидаем быстрое OK, так как достаточно 1 подтверждения от followers
# Сразу делаем чтение с двух followers
get <key> --target B
get <key> --target C
# Ожидаем, что один из followers (тот, который подтвердил запись) вернет новое значение,
# а другой может вернуть stale read
# Ждем 10 секунд
# Делаем повторное чтение с обоих followers
get <key> --target B
get <key> --target C
# Ожидаем, что оба followers теперь вернут новое значение, так как догнали запись в фоне
```
---

