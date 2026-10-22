# Kafka UI (Quarkus + htmx) — design

Datum: 2026-10-22
Status: utkast för godkännande

## Syfte och varför

Ett fristående webb-UI för ett Strimzi-baserat Kafka-kluster, i stil med Kafbat/kafka-ui (design och funktioner), men där **Kafka självt avgör vad användaren får göra**. Kafbat har en egen RBAC-modell i UI:t; vi vill i stället att användarens egen behörighet i Kafka (Keycloak Authorization Services via Strimzi OAuth) styr – UI:t har ingen egen behörighetsmodell.

Det byggs om från admin-delen i `../kroxy-test/kafka-operator/kafka-editor` (React + Quarkus), men frontenden skrivs om i Quarkus/Qute + htmx och flödeseditorn (Kafka Streams-designern) slängs helt.

## Användare

- **Utvecklare och drift (huvudanvändare):** felsöker meddelanden i topics – hittar ett meddelande, ser det avkodat, förstår varför det ser ut som det gör.
- **Testare:** producerar meddelanden (XML, JSON-sträng, Avro) till topics de har behörighet till.
- **Kafka-administratörer:** använder det mer sällan, för översikt (ACL:er, consumer groups, config).

## Mål

1. Visa topics, meddelanden, consumer groups, ACL:er, scheman (Apicurio Registry 3) och klusteröversikt – ungefär som Kafbat.
2. Meddelandevy med **tail** (live) och **regexp-sök på key/value** inom ett tidsintervall.
3. **Automatisk deserialisering** – användaren ska inte behöva välja deserializer.
4. **Produce** av meddelanden (rått eller Avro via registry).
5. **Reset offsets** på consumer groups.
6. Två autentiseringslägen: **mTLS** och **OIDC**.

## Icke-mål

- Flödeseditorn från kafka-editor.
- Skapa/ta bort/ändra topics, topic-config, ACL:er, scheman; ta bort consumer groups; tömma topics. **Enda skrivoperationerna är produce och reset offsets.**
- Kafka Connect, KSQL.
- Flera kluster per installation (en installation per kluster).
- Apicurio Registry v2.
- Protobuf (varken läsa eller producera).
- Egen RBAC/behörighetsmodell i UI:t.

## Anti-mål (blir ett misslyckande även om funktionerna finns)

- Sökningen är långsam, eller söker bara de senaste N meddelandena utan att det framgår.
- Meddelanden visas som obegriplig binärdata när de går att avkoda.
- Användaren får kryptiska fel i stället för t.ex. "du saknar behörighet till topic X".

## Begränsningar

- **Backend:** Quarkus (Java). **Frontend:** server-renderad HTML (Qute) + htmx. Ingen SPA.
- **Kafka:** vanligt Strimzi-kluster.
- **Schema registry:** Apicurio Registry 3, nås via en proxy framför Apicurio som hanterar mTLS och OIDC mot Keycloak (Apicurios egen behörighetsmodell används inte).
- **Keycloak** för inloggning och Keycloak Authorization Services för Kafka-behörighet (Strimzi `KeycloakAuthorizer`).
- **Driftmiljö:** container i Kubernetes, konfiguration via miljövariabler. Certifikat utfärdas av cert-manager.

## Autentiseringslägen

### mTLS-läge
- Används **bara i testmiljöer** för att snabbt få upp ett UI; prod kör alltid OIDC-läge.
- Ingen inloggning i UI:t. Alla som når UI:t får certets rättigheter (inkl. produce och reset offsets). UI:t visar en tydlig banner om att det kör i mTLS-läge utan inloggning. *(bannern: mitt beslut)*
- UI:t kopplar mot Kafka med **sitt eget klientcertifikat** (SSL, mTLS). Alla användare får certets rättigheter.
- Samma klientcert skickas mot **Apicurio-proxyn**.
- Certifikat refereras via Kubernetes-secrets:
  - klientcert: secret-namn + fältnamn för cert och nyckel (default `tls.crt` / `tls.key`, cert-manager-standard),
  - trust: secret-namn + **fältnamn för CA-bundlen** (konfigurerbart, eftersom det exakta namnet varierar, t.ex. `ca-bundle.pem`).

### OIDC-läge
- Användaren loggar in i UI:t mot Keycloak (authorization code flow).
- UI:t kopplar mot Kafka med **användarens access token** via SASL OAUTHBEARER (Strimzi OAuth). Keycloak Authorization Services avgör vad användaren får göra.
- Apicurio-proxyn anropas med **användarens token** (Bearer).
- **Token vidarebefordras som den är, ingen token exchange.** Så hanterar Strimzi OAuth det: listenern validerar tokenets signatur och issuer (`validIssuerUri`, JWKS) och kontrollerar audience bara om `checkAudience=true` (default false). `KeycloakAuthorizer` hämtar sedan användarens grants från Keycloak (UMA, `grant_type=urn:ietf:params:oauth:grant-type:uma-ticket`) med användarens token mot den Kafka-klient som anges i `strimzi.authorization.client.id` – det fungerar oberoende av vilken klient som utfärdade tokenet, så länge det är samma realm. Krav att dokumentera för driften:
  - UI-klienten och Kafka måste ligga i samma realm/issuer.
  - Om listenern har `checkAudience=true`: lägg till en audience-mapper på UI-klienten i Keycloak som lägger Kafka-klienten i `aud`.
  - Om listenern har `userNameClaim`/`customClaimCheck`: tokenet måste innehålla de claimen.
  - Apicurio-proxyns principal-/roll-claim (`PRINCIPAL_CLAIM`) måste finnas i UI-klientens token (mapper i Keycloak).
- Tokenet förnyas via Quarkus OIDC (refresh token). Långlivade Kafka-klienter (tail) återskapas med nytt token när det förnyas. *(mitt beslut)*
- CA-bundlen (samma referens som ovan) används för TLS-trust mot Kafka och proxyn.

## Behörighet i UI:t

- UI:t frågar Kafka i förväg via AdminClients `authorizedOperations` (topic, consumer group, kluster) och **inaktiverar** knappar för produce/reset offsets när rätten saknas, med en förklaring.
- Listor visar det Kafka returnerar (t.ex. bara topics användaren har Describe på).
- Om Kafka ändå nekar en operation visas ett tydligt, begripligt felmeddelande ("Du saknar behörighet att X på Y"), aldrig en råstacktrace.
- Samma princip mot Apicurio-proxyn: 401/403 visas begripligt.

## Funktioner (Kafbat-lik struktur)

Layout som Kafbat: vänster sidomeny, tabeller, detaljvyer med flikar.

- **Översikt / Brokers:** klusterinfo, brokers, grundläggande status (read-only).
- **Topics:** lista (namn, partitioner, replikering, antal meddelanden om möjligt), sök/filter på namn. Detaljvy med flikar:
  - *Översikt:* partitioner, offsets, ISR.
  - *Meddelanden:* se nedan.
  - *Consumers:* consumer groups som läser topicen, med lag.
  - *Settings:* topic-config (read-only).
- **Consumer groups:** lista med state, medlemmar, lag. Detaljvy per partition. **Reset offsets** (till earliest, latest, tidpunkt, specifikt offset) – som Kafbat.
- **Schema registry:** lista artefakter/grupper, detaljvy med versioner och schemainnehåll (read-only).
- **ACL:er:** Kafkas egna ACL:er (AdminClient `describeAcls`), lista med filtrering (principal, resurs, operation), read-only. Alla applikationsklienter kör mTLS och deras behörighet ligger i Kafka-ACL:er – det är dem som är intressanta att se. Keycloak-policyer (som styr OIDC-användarnas gruppbaserade åtkomst) visas **inte**. I OIDC-läge kräver vyn att användaren har Describe på Cluster; annars visas ett begripligt "saknar behörighet".

## Meddelanden

### Bläddra/sök
- Sök inom ett **tidsintervall** (default "senaste timmen"); alternativt från ett offset. Val av partition(er).
- **Regexp på key och/eller value** (även headers är ett rimligt tillägg – byggarens val). Matchning sker mot **avkodat** innehåll (t.ex. JSON-representationen av en Avro-post), inte råa bytes.
- Resultaten **strömmas** in medan sökningen pågår; användaren ser hur många meddelanden som skannats och kan **avbryta**.
- Övre gräns för antal skannade meddelanden och träffar ska finnas och synas för användaren.
- Skydd mot regexp som hänger sig (katastrofal backtracking) – t.ex. tidsgräns per matchning.

### Tail
- Live-ström av nya meddelanden (SSE), med paus/fortsätt och samma filtrering.
- Begränsat antal rader i vyn.

### Meddelandedetalj
- Key, value, headers, partition, offset, timestamp.
- Formaterad och syntaxfärgad visning av JSON och XML.
- Visar **vilken deserializer som valdes och varför** (t.ex. "Avro, schema `group/artifact` v3, contentId 42 från header"). Användaren kan byta deserializer manuellt för ett meddelande om gissningen blev fel.

### Automatisk deserialisering (key och value var för sig)
1. **Apicurio-headers** (`apicurio.key.*` / `apicurio.value.*`, globalId eller contentId) → hämta schemat från registryt via id.
2. Annars **magic byte 0x00** i payloaden → prova 4-byte contentId (Apicurio 3-standard), sedan 8-byte globalId. Räknas som träff bara om id:t finns i registryt.
3. Schematyp avgör avkodning: **Avro** eller **JSON Schema**.
4. Utan schema: **JSON** → **XML** → UTF-8-text → hex.
- Scheman cachas (även "finns inte"-svar, med rimlig TTL). **I OIDC-läge cachas per användare**, eftersom Apicurio-proxyn styr läsrätt per artefakt och roll; ingen ska kunna avkoda med ett schema de inte får läsa. 401/403 från proxyn cachas inte som "finns inte" utan visas som "saknar behörighet till schemat". *(mitt beslut)*
- Avkodningsfel på ett meddelande stoppar aldrig vyn; meddelandet visas med fallback och en förklaring.

## Produce

- Fält: key, value, headers, partition (valfritt).
- **Rått läge:** value skickas som sträng (XML, JSON, text).
- **Avro-läge:** användaren skriver JSON och väljer schema och version från Apicurio. UI:t validerar mot schemat och visar var det är fel, och serialiserar sedan med **Apicurios egen serde** så meddelandet ser ut som från en riktig producent.
- Om schema-id:t skrivs i headers eller i payloaden, om det är contentId eller globalId, och header-namnen styrs av konfiguration (ska matcha era producenter). Default är Apicurio 3-serdens egna defaultvärden. Läsdelen provar alla varianter oavsett inställning. *(mitt beslut)*

## Konfiguration (miljövariabler, ungefärligt)

- Läge: `mtls` | `oidc`.
- Kafka bootstrap.
- Apicurio-proxyns URL.
- Sökvägar till klientcert, nyckel och CA-bundle (PEM).

### Certifikat från Kubernetes-secrets
- Secrets **monteras som volymer** (ingen Kubernetes-klient i appen). Exempel-manifest (Deployment, Service, ConfigMap/env) levereras där man anger:
  - klientcert: secret-namn + fältnamn för cert och nyckel (default `tls.crt` / `tls.key`),
  - trust: secret-namn + **fältnamn för CA-bundlen** (t.ex. `ca-bundle.pem` – namnet är konfigurerbart eftersom det varierar).
  Manifestet mappar fälten till fasta filsökvägar via `items`.
- Filerna laddas om när cert-manager roterar dem (filbevakning; Kafka-klienter återskapas). *(mitt beslut)*
- Keycloak: issuer-URL, client-id, client-secret – OIDC-läge.
- Schema-id-placering vid produce (headers/payload).
- Sökgränser (max skannade meddelanden, tidsgräns).

## Utgångsmaterial

Från `../kroxy-test/kafka-operator/kafka-editor` (Quarkus 3.17-backend):
- `backend/.../admin/serde/MessageDeserializer.java`, `SchemaCache.java`, `Decoders.java` – detektionslogik att bygga vidare på (behöver Apicurio 3, headers, JSON Schema, XML).
- `admin/AdminClientFactory.java` – per-request-klienter med användarens token.
- `admin/service/*` – MessageBrowse, Produce, Group, Acl.
- Flödeseditorn (`interpreter/`, `run/`, `model/`, `RunResource`, och all React-kod) tas inte med.

## Lämnat till byggaren

- Paketstruktur, Quarkus-version (senaste stabila 3.x), Apicurio 3-klient/serde-bibliotek.
- CSS-ramverk och visuell finish (inom Kafbat-likt utseende).
- Livscykel för Kafka-klienter per användare/session och tokenförnyelse för långlivade tail-strömmar.
- Teststrategi (enhetstester + integrationstester mot Strimzi/Keycloak/Apicurio i containrar).
