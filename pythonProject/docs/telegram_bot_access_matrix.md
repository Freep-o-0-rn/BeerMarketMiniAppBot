diff --git a/pythonProject/docs/telegram_bot_access_matrix.md b/pythonProject/docs/telegram_bot_access_matrix.md
new file mode 100644
index 0000000000000000000000000000000000000000..6b0b6848775236c19c47dc1bab94547ad4249e6a
--- /dev/null
+++ b/pythonProject/docs/telegram_bot_access_matrix.md
@@ -0,0 +1,50 @@
+# Telegram bot access matrix
+
+## Roles
+
+- `guest` — только базовый просмотр: прайсы, акции, график.
+- `client` — просмотр своих данных и своей карточки клиента.
+- `sales_rep` — работа со своими клиентами, карточками, просрочкой, переплатами и ТТН.
+- `admin` — полный доступ.
+
+## Message / command entry points
+
+| Entry point | guest | client | sales_rep | admin |
+| --- | --- | --- | --- | --- |
+| `/help`, `▶️ Старт` | ✅ | ✅ | ✅ | ✅ |
+| `📑 Прайсы`, `🎁 Акции`, `🚚 График развоза` | ✅ | ✅ | ✅ | ✅ |
+| `🔎 Поиск`, `🔎 Поиск тары` | ❌ | ✅ (только свои данные) | ✅ | ✅ |
+| `🧾 Общий отчёт`, `/report` | ❌ | ❌ | ❌ | ✅ |
+| `⏰ Просрочено`, `💰 Переплаты` | ❌ | ❌ | ✅ | ✅ |
+| `📦 Тара`, `/tara` | ❌ | ❌ | ❌ | ✅ |
+| `📦 Проверить ТТН` | ❌ | ❌ | ✅ | ✅ |
+| `🔄 Обновить`, `/refresh`, `/refresh_tara` | ❌ | ❌ | ❌ | ✅ |
+| `🏢 Клиенты`, `🏢 Моя карточка` | ❌ | ✅ (view only) | ✅ | ✅ |
+| `🛠 Техники` | ❌ | ❌ | ❌ | ✅ |
+| `👥 Пользователи`, `/users` | ❌ | ❌ | ❌ | ✅ |
+
+## Callback namespaces
+
+| Namespace | guest | client | sales_rep | admin |
+| --- | --- | --- | --- | --- |
+| `upd:*` | ❌ | ❌ | ❌ | ✅ |
+| `ttn:*` | ❌ | ❌ | ✅ | ✅ |
+| `cc:view:*`, `cc:list*` | ❌ | ✅ | ✅ | ✅ |
+| `cc:new`, `cc:import:*`, `cc:edit:*`, `cc:editfield:*`, `cc:edittech:*`, `cc:addcontact:*`, `cc:net:*`, `cc:del:*` | ❌ | ❌ | ✅ | ✅ |
+| `tc:*` | ❌ | ❌ | ❌ | ✅ |
+| `usr:*` | ❌ | ❌ | ❌ | ✅ |
+
+## FSM flows
+
+| FSM flow | guest | client | sales_rep | admin |
+| --- | --- | --- | --- | --- |
+| TTN (`TTNStates.*`) | ❌ | ❌ | ✅ | ✅ |
+| Карточка клиента — создание/редактирование/контакты (`ClientCardStates.*`) | ❌ | ❌ | ✅ | ✅ |
+| Техники (`TechnicianStates.*`) | ❌ | ❌ | ❌ | ✅ |
+| Пользователи (`AdminUserEditStates.*`) | ❌ | ❌ | ❌ | ✅ |
+
+## Notes
+
+- `client` больше не получает управляющие кнопки в карточке клиента.
+- При отказе по правам бот должен показать понятное сообщение и вернуть пользователя в доступное меню.
+- Роль по умолчанию для неизвестного/неназначенного пользователя — `guest`.
