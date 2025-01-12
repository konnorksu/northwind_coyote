# northwind_coyote
# **ETL proces datasetu NorthWind**

Tento článok opisuje implementáciu procesu ETL v programe Snowflake na analýzu údajov zo súboru údajov **NorthWind**. Cieľom je preskúmať predajné a demografické údaje. Výsledný dátový model umožňuje viacrozmernú analýzu a vizualizáciu kľúčových metrík.



---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať údaje týkajúce sa predaja, výrobkov a dodávateľov. Táto analýza umožňuje identifikovať trendy v preferenciách výrobkov, najobľúbenejších dodávateľov a obľúbené mestá.

Zdrojové dáta pochádzajú z Git datasetu dostupného [tu](https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/northwind-pubs). Dataset obsahuje osem hlavných tabuliek:
- `categories`
- `customers`
- `employees`
- `orderdataisl`
- `orders`
- `products`
- `shippers`
- `supplies`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/konnorksu/northwind_coyote/blob/main/Northwind_ERD.png?raw=true" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma NorthWind</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_orders`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_shippers`**: Obsahuje informácie o doručovateľov (meno, čislo telefonu).
- **`dim_customer`**: Obsahuje údaje o zákazníkoch (meno, adresa, mesto, krajina, PSČ).
- **`dim_supplier`**: Obsahuje údaje o dodávateľoch (meno, adresa, mesto, krajina, PSČ).
- **`dim_employees`**: Obsahuje informácie o zamestnancoch, ktorí spracovali objednávky (dátum narodenia, meno, priezvisko).
- **`dim_products`**: Obsahuje informácie o produktoch (názov, cena, názov kategórie)
- **`dim_date`**: Zahrňuje informácie o dátumoch objednávok (deň, mesiac, rok).

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/konnorksu/northwind_coyote/blob/main/starsceme_nortwind.png?raw=true" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre NorthWind</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces obsahuje fázy: `extrahovanie`, `transformácia` a `načítanie`. Dáta boli najprv nahraté do Snowflake, následne vyčistené a transformované pre finálnu analýzu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `coyote_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE coyote_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```
Súbory pre produkty, objednávky, zákazníkov a ďalšie entity boli následne nahrané do príslušných staging tabuliek prostredníctvom príkazu COPY INTO. Tento príkaz bol použitý pre každú tabuľku samostatne:

```sql
COPY INTO products_staging
FROM @coyote_stage/products.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

Parameter `SKIP_HEADER` zabezpečil ignorovanie hlavičkových riadkov v zdrojových súboroch, pričom konzistentné formátovanie zabezpečilo bezchybné nahrávanie.

---
### **3.1 Transfor (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku. `dim_products` obsahuje údaje o názve, cene a názve kategórie.
```sql
CREATE OR REPLACE TABLE dim_products AS
SELECT 
    p.ProductId AS productId,
    p.productName,
    p.price,
    c.categoryName,
FROM products_staging p
JOIN categories_staging c ON p.categoryId = c.CategoryId;

```
Dimenzie zákazníkov a dodávateľov. Tabuľky obsahujú údaje o menach, meste, krajine a PSČ.

```sql
CREATE OR REPLACE TABLE dim_customers AS 
SELECT
    CustomerId as CustomerId,
    CustomerName as CustomerName,
    address as address,
    city as city,
    country as country,
    PostalCode as PostalCode
FROM customers_staging;
```

```sql
CREATE OR REPLACE TABLE dim_suppliers AS 
SELECT
    SupplierId as SupplierId,
    SupplierName as SupplierName,
    ContactName as ContactName,
    address as address,
    city as city,
    country as country,
    PostalCode as PostalCode
FROM suppliers_staging;
```
Dimenzia zamestnancov `dim_employees` obsahuje údaje o dátume narodenia, mene a priezvisku.

```sql
CREATE OR REPLACE TABLE dim_employees AS
SELECT 
    EmployeeId as employeeId,
    FirstName as FirstName,
    LastName as LastName,
    BirthDate as BirthDate
FROM employees_staging;
```

Dimenzia `dim_shippers` obsahuje údaje o mene a čislo telefona.

```sql
CREATE OR REPLACE TABLE dim_shippers AS
SELECT 
    ShipperId,
    ShipperName,
    Phone
FROM shippers_staging;
```

Dimenzia dátumu `dim_date` analyzované dátumy objednávok na odvodenie roka, mesiaca a dňa.

```sql
CREATE OR REPLACE TABLE dim_date AS
SELECT
    DISTINCT
    DATE_PART('year', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS year,
    DATE_PART('month', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS month,
    DATE_PART('day', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS day,
FROM orders_staging;
```


Faktová tabuľka `fact_orders` bola vytvorená na ukladanie kľúčových transakčných dát, spájajúcich dimenzie s odvodenými metrikami, ako sú cena a množstvo produktov.
```sql
CREATE OR REPLACE TABLE fact_orders AS
SELECT
    o.OrderId AS orderId,
    od.quantity AS Quantity,
    ps.price AS unitPrice,
    (od.quantity * ps.price) AS totalPrice,
    b.OrderDeteilId AS bridgeId,
    TO_DATE(TO_TIMESTAMP(o.orderDate, 'YYYY-MM-DD HH24:MI:SS')) AS DateOfOrder,
    e.employeeId,
    ps.supplierId AS supplierId,
    c.customerId AS customerId
FROM orders_staging o
JOIN bridge b ON o.orderId = b.orderId
JOIN orderdetails_staging od ON b.orderId = od.orderid AND b.productId = od.productId
JOIN products_staging ps ON b.productId = ps.productId
JOIN dim_employees e ON o.employeeId = e.employeeId
JOIN dim_customers c ON o.customerId = c.customerId;
```

---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
DROP TABLE IF EXISTS products_staging;
DROP TABLE IF EXISTS orders_staging;
DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS categories_staging;
DROP TABLE IF EXISTS employees_staging;
DROP TABLE IF EXISTS orderdetails_staging;
DROP TABLE IF EXISTS shippers_staging;
DROP TABLE IF EXISTS suppliers_staging;
```
Proces ETL v softvéri Snowflake umožnil spracovanie nespracovaných údajov z formátu `.csv` do viacrozmerného modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analyzovať preferencie zákazníkov a štatistiky predaja a poskytuje základ pre vizualizácie a reporty.

---
## **4 Vizualizácia dát**

Dashboard obsahuje `5 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa objednávok, zákazníkov, produktov a predaja.

<p align="center">
  <img src="https://github.com/konnorksu/northwind_coyote/blob/main/grafy/grafy.png?raw=true" alt="Grafy">
  <br>
  <em>Obrázok 3 Dashboard NorthWind datasetu</em>
</p>

---
### **Graf 1: Objem predaja podľa dodávateľov**
Táto vizualizácia zobrazuje celkový objem predaja jednotlivých dodávateľov. Pomáha identifikovať najvýznamnejších dodávateľov, pričom `Aux joyeux ecclésiastiques` výrazne prevažuje.

```sql
SELECT 
    ds.SupplierName, 
    SUM(fo.totalPrice) AS total_sales
FROM fact_orders fo
JOIN dim_suppliers ds ON fo.supplierId = ds.SupplierId
GROUP BY ds.SupplierName
ORDER BY total_sales DESC;
```
<p align="center">
  <img src="https://github.com/konnorksu/northwind_coyote/blob/main/grafy/graf3.png?raw=true" alt="Graf 1">
  <br>
  <em>Graf 1</em>
</p>

---
### **Graf 2: Top 5 zamestnancov podľa počtu spracovaných objednávok**
Graf hodnotí efektivitu zamestnancov na základe počtu spracovaných objednávok, čo je užitočné napríklad pri prideľovaní bonusov.

```sql
SELECT 
    de.FirstName || ' ' || de.LastName AS employee_name, 
    COUNT(fo.orderId) AS total_orders
FROM fact_orders fo
JOIN dim_employees de ON fo.employeeId = de.employeeId
GROUP BY employee_name
ORDER BY total_orders DESC
LIMIT 5;
```
<p align="center">
  <img src="https://github.com/konnorksu/northwind_coyote/blob/main/grafy/graf4.png?raw=true" alt="Graf 2">
  <br>
  <em>Graf 2</em>
</p>

---
### **Graf 3: Pomer objednávok podľa miest**
Zobrazuje geografické rozloženie objednávok, čo umožňuje identifikovať najvýznamnejšie regióny pre podnikanie.


```sql
SELECT 
    dc.city, 
    COUNT(fo.orderId) AS total_orders
FROM fact_orders fo
JOIN dim_customers dc ON fo.customerId = dc.customerId
GROUP BY dc.city
ORDER BY total_orders DESC;
```
<p align="center">
  <img src="https://github.com/konnorksu/northwind_coyote/blob/main/grafy/graf5.png?raw=true" alt="Graf 3">
  <br>
  <em>Graf 3</em>
</p>

---
### **Graf 4: Celkový predaj v jednotlivých rokoch**
Graf sleduje dynamiku predaja v priebehu rokov, čím umožňuje vyhodnotiť trendy v predaji.

```sql
SELECT 
    DATE_PART('year', DateOfOrder) AS year, 
    SUM(totalPrice) AS total_sales
FROM fact_orders
GROUP BY year
ORDER BY year;
```
<p align="center">
  <img src="https://github.com/konnorksu/northwind_coyote/blob/main/grafy/graf2.png?raw=true" alt="Graf 4">
  <br>
  <em>Graf 4</em>
</p>

---
### **Graf 5: Top 5 zákazníkov podľa objemu nákupu**
Vizualizácia identifikuje najhodnotnejších zákazníkov na základe celkového objemu nákupov, čo môže byť využité na personalizáciu ponúk.

```sql
SELECT 
    dc.CustomerName, 
    SUM(fo.totalPrice) AS total_purchases
FROM fact_orders fo
JOIN dim_customers dc ON fo.customerId = dc.customerId
GROUP BY dc.CustomerName
ORDER BY total_purchases DESC
LIMIT 5;
```
<p align="center">
  <img src="https://github.com/konnorksu/northwind_coyote/blob/main/grafy/graf1.png?raw=true" alt="Graf 5">
  <br>
  <em>Graf 5</em>
</p>

---

Dashboard poskytuje komplexný prehľad o dátach, ktorý je kľúčový pre optimalizáciu marketingových stratégií a zlepšenie služieb.

---

**Autor:** Kseniya Bibetka
