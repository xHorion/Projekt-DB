# ELT proces datasetu "Postcode Sector Weather Forecasts"

Tento repozitár ukazuje spôsob implementácie procesu ELT v Snowflake a vytvorenie dátového skladu so schémou Star Schema. Pre náš projekt sme vybrali dátový súbor - **Postcode Sector Weather Forecasts – Sample** . 
Cieľom projektu je vybudovať spoľahlivú analytickú platformu, ktorá transformuje zložité meteorologické údaje na jednoduché a zrozumiteľné ukazovatele pre rozhodovanie v logistike, maloobchode alebo správe mesta.
Vyvinutá architektúra **Star Schema** premieňa surové meteorologické prognózy na štruktúrovaný analytický aktívum.

---
## 1. Úvod a popis zdrojových dát

### 1.1 Prečo práve tento dátový súbor?
Tento dataset používame z nasledujúcich dôvodov:

* **Geopriestorová zložka**: Na rozdiel od bežných tabuliek je tento dataset viazaný na poštové sektory (postcode sectors) a obsahuje súradnice. To umožnilo realizovať analýzu s vysokou presnosťou pre konkrétnu oblasť, a nie len pre mesto.
* **Spoľahlivosť zdroja**: Údaje poskytla Met Office (národná meteorologická služba Spojeného kráľovstva), čo zaručuje ich realistickosť a súlad s priemyselnými štandardmi.
* **Rozmanitosť metrík**: Prítomnosť nielen teploty, ale aj rýchlosti vetra, viditeľnosti a pocitu teploty (feels_like) umožnila vytvoriť rôznorodé vizualizácie a analýzy.
* **Zrozumiteľnosť pre všetkých**: Počasie je téma, ktorá je blízka každému. Každý rozumie, čo je teplota alebo silný vietor, takže výsledky projektu je ľahké vysvetliť aj ľuďom, ktorí nie sú technicky zdatní.
* **Všetko na jednom mieste**: Tu sú čísla (teplota), text (opis počasia) aj geografia (súradnice). To nám umožnilo použiť rôzne analytické nástroje v jednom projekte.

### 1.2 Podporované biznis procesy
Tento projekt poskytuje dáta, ktoré sú kľúčové pre nasledujúce oblasti:

### 1.3 Ciele
**Cieľom** tohto projektu je analyzovať predpovede počasia na úrovni poštových sektorov a vytvoriť dátovú infraštruktúru pre:

* **Analýzu lokálnych podmienok** (teplota, vietor, viditeľnosť) v konkrétnych oblastiach.
* **Sledovanie teplotných zmien** (delta teploty) medzi po sebe nasledujúcimi dňami.
* **Kategorizáciu rizík** spojených s počasím (napr. silný vietor alebo znížená viditeľnosť).

### 1.4 Dátová architektúra
Zdrojom dátového súboru je [Snowflake Marketplace](https://app.snowflake.com/marketplace/listing/GZTDZJKVBD/met-office-postcode-sector-weather-forecasts-sample?search=Weather). Dataset obsahuje tri tabuľky:

* `free_view_daily` - Meteorologické údaje za celý deň
* `free_view_hourly` - Meteorologické údaje za hodinu
* `free_view_three_hourly` - Meteorologické údaje za tri hodiny

Všetky tieto tabuľky sú navzájom prepojené. Každá tabuľka zobrazuje rovnaké údaje, avšak za rôzne časové obdobia. V našom projekte používame práve tabuľku `free_view_daily` a `free_view_hourly. V podstate používame tabuľku „free_view_daily“, pretože práve meteorologické údaje za celý deň sú vhodnejšie na porovnávanie, efektívne sa ukladajú a ideálne sa hodia pre obchodné cykly (logistika, plánovanie objednávok alebo zmien práce) a mnoho ďalších vecí. Tabuľku „free_view_hourly“ používame len na výpočet priemernej dennej teploty.

Cieľom procesu ELT bolo pripraviť, transformovať a sprístupniť tieto údaje na účely multidimenzionálnej analýzy.

---

### **ERD Diagram** 

Surové údaje sú usporiadané v relačnej modeli, ktorá je znázornená na entítno-relačnom diagramu (ERD) nižšie:

**ТУТ ФОТКА СИРОЇ СХЕМИ С ОПИСОМ**

---

## 2. Dimenzionálny model

Používame Star Schema podľa Kimballovej metodológie, ktorá obsahuje 1 tabuľku faktov , ktorá je prepojená s nasledujúcimi 4 dimenziami.

### 2.1 Tabuľka faktov

**fact_weather_forecast**
* **Primárny kľúč (PK)**: `fact_key`,
* **Cudzie kľúče (FK)**:
  * `dim_date_key`
  * `dim_location_key`
  * `dim_weather_type_key`
  * `dim_wind` **!!!**
* **Hlavné metriky**:
  * `max_temperature_day` / `max_temperature_night` - denné a nočné maximá.
  * `avg_temperature` — priemerná denná teplota.
  * `max_feels_like_temperature_day` / `min_feels_like_temperature_night` — pocitová teplota.
  * `past_day_temperature_delta` — zmena priemernej teploty v porovnaní s predchádzajúcim dňom.
  * `visibility_midday` / `visibility_midnight` — viditeľnosť na poludnie a o polnoci (dôležité pre dopravu).
  * `wind_speed_midday` / `wind_speed_midnight` — rýchlosť vetra v kľúčových časoch dňa.
  * `wind_direction_midday` / `wind_direction_midnight` — smer vetra.
  * `wind_speed_category` — kategorizácia sily vetra na základe rýchlosti.

### 2.2 Tabuľky dimenzií

**dim_location** 
* **Obsah**: Obsahuje podrobné informácie o lokalite (poštové smerovacie číslo, súradnicovú výšku a šírku).
* **Vzťah k faktom**: 1:N
* **Typ SCD**: Typ 0

**dim_weather_type** 
* **Obsah**: obsahuje informácie o type počasia (jeho kód, popis).
* **Vzťah k faktom**: 1:N
* **Typ SCD**: Typ 0

**dim_date** 
* **Obsah**: obsahuje podrobné informácie o dátume, kedy sa vykonalo meranie a záznam (úplný dátum, deň, deň v týždni, mesiac, rok, štvrťrok a sezóna).
* **Vzťah k faktom**: 1:N
* **Typ SCD**: Typ 0

**dim_wind** – obsahuje úplné informácie o vetre (jeho rýchlosť, smer)
* **Obsah**: Obsahuje podrobné informácie o lokalite (poštové smerovacie číslo, súradnicovú výšku a šírku).
* **Vzťah k faktom**: 1:N
* **Typ SCD**: Typ 0

### 2.3 ERD Diagram
Štruktúra hotového hviezdneho modelu je znázornená na nižšie uvedenom diagramu. Diagram ukazuje vzťahy medzi tabuľkou faktov a dimenziami, čo pomáha ľahšie pochopiť a používať model.

**ТУТ ФОТКА ЗВЕЗДОЧНОЇ СХЕМИ З ОПИСОМ**

---
## 3. ELT proces v Snowflake

ETL proces pozostáva z troch hlavných fáz: extrahovanie (Extract), načítanie (Load) a transformácia (Transform). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

### 3.1 Extract (Extrahovanie dát)

Keďže používame bezplatný dátový súbor zo Snowflake Marketplace, stačí len importovať údaje do našich tabuliek pri vytváraní dočasnej tabuľky. 
Najskôr vytvoríme správnu „štruktúru“ pre našu budúcu tabuľku a vyplníme ju surovými údajmi.
Najprv vykonáme tieto kroky s tabuľkou `STAGING_HOURLY_WEATHER`, pretože túto tabuľku budeme potrebovať na výpočet priemernej teploty:

```SQL
CREATE OR REPLACE TABLE STAGING_HOURLY_WEATHER AS
SELECT 
    "PC_SECT" AS pc_sect,
    "Validity_date_and_time" AS validity_date_and_time,
    "Issued_at" AS issued_at,
    "Screen_temperature" AS screen_temperature,
    "POINT" AS point
FROM POSTCODE_SECTOR_WEATHER_FORECASTS__SAMPLE.PCSECT_FORECAST."free_view_hourly";
```

Ako vidíme, na vyplnenie našej tabuľky používame nasledujúci spôsob: `POSTCODE_SECTOR_WEATHER_FORECASTS__SAMPLE.PCSECT_FORECAST.„free_view_hourly“`
To je náš dátaset. Databáza: `POSTCODE_SECTOR_WEATHER_FORECASTS__SAMPLE`,
schéma: `PCSECT_FORECAST`, tabuľka: `„free_view_hourly”`.

Teraz robíme úplne rovnaké kroky s vytvorením štruktúry a vyplnením údajmi, ale už pre tabuľku `STAGING_DAILY_WEATHER`.

```SQL
CREATE OR REPLACE TABLE STAGING_DAILY_WEATHER AS
SELECT 
    "PC_SECT" AS pc_sect,
    "Validity_date" AS validity_date,
    "Issued_at" AS issued_at,
    "Max_temperature_day" AS max_temperature_day,
    "Min_temperature_night" AS min_temperature_night,
    "Max_feels_like_temperature_day" AS max_feels_like_temperature_day,
    "Min_feels_like_temperature_night" AS min_feels_like_temperature_night,
    "Visibility_Approx_Local_Midday" AS visibility_approx_local_midday,
    "Visibility_Approx_Local_Midnight" AS visibility_approx_local_midnight,
    "Significant_weather_day" AS significant_weather_day,
    "Significant_weather_type_day" AS significant_weather_type_day,
    "Significant_weather_night" AS significant_weather_night,
    "Significant_weather_type_night" AS significant_weather_type_night,
    "Wind_speed_Approx_Local_Midday" AS wind_speed_approx_local_midday,
    "Wind_direction_Approx_Local_Midday" AS wind_direction_approx_local_midday,
    "Wind_speed_Approx_Local_Midnight" AS wind_speed_approx_local_midnight,
    "Wind_direction_Approx_Local_Midnight" AS wind_direction_approx_local_midnight,
    "POINT" AS point
FROM POSTCODE_SECTOR_WEATHER_FORECASTS__SAMPLE.PCSECT_FORECAST."free_view_daily";
```

Cesta je tu presne taká istá, ale tabuľka je už iná. V našom prípade je to teraz `„free_view_daily”`. Práve s touto tabuľkou budeme ďalej pracovať. Práve ona obsahuje „surové” údaje, ktoré potrebujeme.

Ďalej vytvoríme prázdne tabuľky dimenzií, do ktorých budeme v budúcnosti vkladať už pripravené údaje.

`DIM_LOCATION`

```SQL
CREATE OR REPLACE TABLE DIM_LOCATION (
    location_key INT PRIMARY KEY AUTOINCREMENT,
    postcode VARCHAR(20),
    latitude FLOAT,
    longitude FLOAT
);
```
`DIM_WEATHER_TYPE`

```SQL
CREATE OR REPLACE TABLE DIM_WEATHER_TYPE (
    weather_type_key INT PRIMARY KEY AUTOINCREMENT,
    weather_type_code INT,
    weather_description VARCHAR(50)
);
```

`DIM_DATE`

```SQL
CREATE OR REPLACE TABLE DIM_DATE (
    date_key INT PRIMARY KEY, -- Format YYYYMMDD
    date DATE,
    day INT,
    week_day INT,
    month INT,
    year INT,
    quarter INT,
    season VARCHAR(15)
);
```

`DIM_WIND_TYPE`

```SQL
CREATE OR REPLACE TABLE DIM_WIND_TYPE (
    wind_type_key INT PRIMARY KEY AUTOINCREMENT,
    wind_direction VARCHAR(5), -- N, NE, E, SE, S, SW, W, NW, N
    wind_speed_category VARCHAR(15)
);
```

Na samom konci vytvárame už tabuľku faktov `FACT_WEATHER_FORECAST` s konečnou štruktúrou:

```SQL

CREATE OR REPLACE TABLE FACT_WEATHER_FORECAST (
    id INT PRIMARY KEY AUTOINCREMENT,
    
    max_temperature_day FLOAT,
    min_temperature_day FLOAT,
    
    avg_temperature_day FLOAT,
    past_day_temperature_delta FLOAT,
    
    max_feels_like_temperature_day FLOAT,
    min_feels_like_temperature_night FLOAT,
    
    visibility_midday FLOAT,
    visibility_midnight FLOAT,
    
    wind_direction_midday FLOAT,
    wind_direction_midnight FLOAT,
    wind_speed_midday FLOAT,
    wind_speed_midnight FLOAT,
    
    dim_date_key INT,
    dim_location_key INT,
    dim_weather_type_key_midday INT,
    dim_weather_type_key_midnight INT,
    dim_wind_type_key_midday INT,
    dim_wind_type_key_midnight INT,
    
    forecast_issued_at TIMESTAMP_NTZ 
);
```
---

### Funckie

V našom projekte vytvárame funkcie samostatne, namiesto toho, aby sme ich robili priamo v tabuľkách. Robíme to preto, aby sme v budúcnosti nemuseli do každej tabuľky zapisovať obrovské množstvo textu, čím ušetríme čas a zmenšíme veľkosť nášho kódu. Pri akýchkoľvek zmenách v obchodnej logike (napr. kategória „Strong“ sa zmení z 29 na 30) bude stačiť zmeniť len niekoľko znakov v jednej funkcii. Je to oveľa jednoduchšie a rýchlejšie, ako to meniť v každej tabuľke, kde to používame.
Funkcie sú uvedené nižšie:

`get_wind_direction` - funkcia, ktora funguje ako digitálny kompas. Prijíma azimut v stupňoch (od 0 do 360) a vráti skrátený názov svetovej strany. Rozdelí kruh na 8 sektorov po 45 stupňoch a pomocou CASE priradí konkrétny názov v tabuľke.

```SQL
CREATE OR REPLACE FUNCTION get_wind_direction(deg FLOAT)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    CASE 
        WHEN deg >= 337.5 OR deg < 22.5 THEN 'N'
        WHEN deg >= 22.5 AND deg < 67.5 THEN 'NE'
        WHEN deg >= 67.5 AND deg < 112.5 THEN 'E'
        WHEN deg >= 112.5 AND deg < 157.5 THEN 'SE'
        WHEN deg >= 157.5 AND deg < 202.5 THEN 'S'
        WHEN deg >= 202.5 AND deg < 247.5 THEN 'SW'
        WHEN deg >= 247.5 AND deg < 292.5 THEN 'W'
        WHEN deg >= 292.5 AND deg < 337.5 THEN 'NW'
        ELSE 'Unknown'
    END
$$;
```

`get_wind_category` - funkcia, ktora klasifikuje silu vetra na základe jeho rýchlosti. Je to zjednodušená verzia Beaufortovej stupnice. Skupinuje nepretržité číselné hodnoty rýchlosti do pevných diskrétnych kategórií.

```SQL
CREATE OR REPLACE FUNCTION get_wind_category(speed FLOAT)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    CASE 
        WHEN speed < 11 THEN 'Light'
        WHEN speed BETWEEN 11 AND 28 THEN 'Moderate'
        WHEN speed BETWEEN 29 AND 49 THEN 'Strong'
        ELSE 'Gale'
    END
$$;
```
---

### 3.2 Load (Načítanie dát)

Po vytvorení všetkých tabuliek konečne prejdeme k fáze Load. Práve v tejto fáze už surové údaje transformujeme do architektúry **Star Schema**, vyplňujúc tabuľky dimenzií.

V tabuľke `DIM_LOCATION` sa extrahujú jedinečné poštové smerovacie čísla. Pomocou funkcií ST_X a ST_Y sa geografický bod (POINT) rozdelí na dĺžku a šírku:

```SQL
INSERT INTO DIM_LOCATION (postcode, longitude, latitude)
SELECT DISTINCT 
    pc_sect AS postcode,
    ST_X(point) AS longitude,
    ST_Y(point) AS latitude
FROM STAGING_DAILY_WEATHER;
```

V tabuľke `DIM_DATE` sa vytvára kalendár. Pre každý dátum sa vypočíta rok, mesiac, štvrťrok a určí sa sezóna (zima, jar atď.) pomocou konštrukcie CASE:

```SQL
INSERT INTO DIM_DATE (date_key, date, day, month, year, quarter, season)
SELECT DISTINCT 
    TO_NUMBER(TO_CHAR(validity_date, 'YYYYMMDD')) AS date_key,
    validity_date AS date,
    DAY(validity_date) AS day,
    MONTH(validity_date) AS month,
    YEAR(validity_date) AS year,
    QUARTER(validity_date) AS quarter,
    CASE 
        WHEN MONTH(validity_date) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(validity_date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(validity_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Autumn'
    END AS season
FROM STAGING_DAILY_WEATHER;
```

V tabuľke `DIM_WEATHER_TYPE` sa zhromažďujú jedinečné kódy počasia a ich popisy z oboch stĺpcov (deň a noc) prostredníctvom UNION:

```SQL
INSERT INTO DIM_WEATHER_TYPE (weather_type_code, weather_description)
SELECT DISTINCT significant_weather_day, significant_weather_type_day FROM STAGING_DAILY_WEATHER
UNION
SELECT DISTINCT significant_weather_night, significant_weather_type_night FROM STAGING_DAILY_WEATHER;
```

V tabuľke `DIM_WIND_TYPE` sa tu používajú naše funkcie, ktoré sme vytvorili skôr. Systém vezme surové údaje o vetre, prejde ich cez get_wind_direction a get_wind_category a uloží jedinečné kombinácie (napríklad „N“ + „Light“):

```SQL
INSERT INTO DIM_WIND_TYPE (wind_direction, wind_speed_category)
WITH All_Wind_Data AS (
    SELECT wind_direction_approx_local_midday as deg, wind_speed_approx_local_midday as speed FROM STAGING_DAILY_WEATHER
    UNION ALL
    SELECT wind_direction_approx_local_midnight as deg, wind_speed_approx_local_midnight as speed FROM STAGING_DAILY_WEATHER
)
SELECT DISTINCT
    get_wind_direction(deg) as direction,
    get_wind_category(speed) as category 
FROM All_Wind_Data
WHERE deg IS NOT NULL;
```
---

### 3.3 Transform (Transformácia dát)

V nižšie uvedenom SQL dotaze vykonávame niekoľko procesov (označených chronologicky):

1. **Hodinová agregácia** (`Hourly_Agregation`): Používame `QUALIFY ROW_NUMBER()`, aby sme vybrali len najnovšie prognózy `issued_at` a ignorovali zastarané údaje. Ďalej sa vypočíta priemerná teplota za deň na základe hodinových záznamov.
2. **Čistenie denných údajov** (`Daily_Clean`): Spojenie denných ukazovateľov s výsledkami hodinovej agregácie. Opätovná deduplikácia na zabezpečenie jedinečnosti záznamov (jedna oblasť – jeden dátum).
3. **Analýza časových radov** (`Daily_With_Lag`): Používa sa okenná funkcia `LAG()` na získanie teploty za predchádzajúci deň. To umožňuje vypočítať ukazovateľ `Temperature Delta` (dynamika zmeny počasia).
4. **Konečné vyhľadávanie a vloženie**: Všetky textové atribúty sa nahradia náhradnými kľúčmi (ID) z tabuliek meraní prostredníctvom `JOIN`. Úplne pripravené údaje sa zapíšu do tabuľky faktov `FACT_WEATHER_FORECAST`.

```SQL
INSERT INTO FACT_WEATHER_FORECAST (
    max_temperature_day, min_temperature_day, avg_temperature_day,
    past_day_temperature_delta, max_feels_like_temperature_day, min_feels_like_temperature_night, 
    wind_speed_midday, wind_speed_midnight, wind_direction_midday, wind_direction_midnight,
    visibility_midday, visibility_midnight,
    dim_date_key, dim_location_key, dim_weather_type_key_midday, dim_weather_type_key_midnight,
    dim_wind_type_key_midday, dim_wind_type_key_midnight,
    forecast_issued_at
)
WITH Hourly_Agregation AS (
    SELECT 
        DATE(validity_date_and_time) as v_date,
        pc_sect,
        ROUND(AVG(screen_temperature), 2) as daily_avg
    FROM (
        SELECT * FROM STAGING_HOURLY_WEATHER
        QUALIFY ROW_NUMBER() OVER (PARTITION BY pc_sect, validity_date_and_time ORDER BY issued_at DESC) = 1
    )
    GROUP BY v_date, pc_sect
),
Daily_Clean AS (
    SELECT 
        d.*,
        h.daily_avg
    FROM STAGING_DAILY_WEATHER d
    LEFT JOIN Hourly_Agregation h ON d.validity_date = h.v_date AND d.pc_sect = h.pc_sect 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY d.pc_sect, d.validity_date ORDER BY d.issued_at DESC) = 1 
),
Daily_With_Lag AS (
    SELECT 
        dc.*,
        LAG(dc.daily_avg) OVER (PARTITION BY dc.pc_sect ORDER BY dc.validity_date) as prev_avg
    FROM Daily_Clean dc
)
SELECT 
    df.max_temperature_day,
    df.min_temperature_night,
    df.daily_avg,
    ABS(df.daily_avg - COALESCE(df.prev_avg, df.daily_avg)), 
    df.max_feels_like_temperature_day,
    df.min_feels_like_temperature_night,
    df.wind_speed_approx_local_midday,
    df.wind_speed_approx_local_midnight,
    df.wind_direction_approx_local_midday,
    df.wind_direction_approx_local_midnight,
    df.visibility_approx_local_midday,
    df.visibility_approx_local_midnight,
    TO_NUMBER(TO_CHAR(df.validity_date, 'YYYYMMDD')),
    l.location_key,
    wd.weather_type_key,
    wn.weather_type_key,
    w_mid.wind_type_key,
    w_night.wind_type_key,
    df.issued_at
FROM Daily_With_Lag df
JOIN DIM_LOCATION l ON df.pc_sect = l.postcode
JOIN DIM_WEATHER_TYPE wd ON df.significant_weather_day = wd.weather_type_code
JOIN DIM_WEATHER_TYPE wn ON df.significant_weather_night = wn.weather_type_code
LEFT JOIN DIM_WIND_TYPE w_mid ON 
    w_mid.wind_direction = get_wind_direction(df.wind_direction_approx_local_midday) AND 
    w_mid.wind_speed_category = get_wind_category(df.wind_speed_approx_local_midday)
LEFT JOIN DIM_WIND_TYPE w_night ON 
    w_night.wind_direction = get_wind_direction(df.wind_direction_approx_local_midnight) AND 
    w_night.wind_speed_category = get_wind_category(df.wind_speed_approx_local_midnight);
```

Prečo je ten dotaz taký veľký? Zlúčili sme niekoľko dotazov na transformáciu do jedneho veľkeho a tu je dôvod:

* **Výkonnosť** (`Query Optimization`): Cloudová databáza Snowflake „vidí“ celú požiadavku ako celok. Optimalizátor môže lepšie rozdeľovať zdroje, pochopiť, ktoré údaje sú potrebné len raz, a vykonať všetko v operačnej pamäti bez zbytočného zápisu na disk.
* **Atómovosť** (`Data Integrity`): Funguje to ako jedna transakcia. Buď sa všetky údaje spracovali a vložili do tabuľky faktov, alebo sa nič nestalo. Nebude existovať poloprázdna tabuľka. 
* **Úspora miesta a peňazí**: Nevytvárate dočasné tabuľky (TEMP TABLES), ktoré zaberali miesto v úložisku a vyžadovali samostatné príkazy na odstránenie.
* **Kontext LAG a okenných funkcií**: Funkcie ako LAG vyžadujú zoradený a čistý súbor údajov. Použitie CTE umožňuje pripraviť tento ideálny súbor „za behu“.

Po úspešnom nahratí údajov do tabuľky môžeme bez obáv odstrániť dočasné tabuľky, aby sme optimalizovali využitie úložiska:

```SQL
DROP TABLE IF EXISTS STAGING_HOURLY_WEATHER;
DROP TABLE IF EXISTS STAGING_DAILY_WEATHER;
```

---

## 4. Vizualizácia dát
