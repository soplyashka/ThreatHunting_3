# Анализ данных сетевого трафика с использованием аналитической
in-memory СУБД DuckDB
Сафрыгина Анастасия

## Цель работы

1.  Изучить возможности СУБД DuckDB для обработки и анализ больших
    данных
2.  Получить навыки применения DuckDB совместно с языком
    программирования R
3.  Получить навыки анализа метаинфомации о сетевом трафике
4.  Получить навыки применения облачных технологий хранения, подготовки
    и анализа данных: Yandex Object Storage, Rstudio Server.

## Ход работы

Подключение через SSH.

    ssh user61@62.84.123.211 -i "C:\Users\Анастасия\Desktop\rstudio.key" -L 8787:127.0.0.1:8787

### Шаг 1. Импорт данных

``` r
library(duckdb)
```

    Loading required package: DBI

``` r
library(dplyr)
```


    Attaching package: 'dplyr'

    The following objects are masked from 'package:stats':

        filter, lag

    The following objects are masked from 'package:base':

        intersect, setdiff, setequal, union

``` r
library(tidyverse)
```

    ── Attaching core tidyverse packages ──────────────────────── tidyverse 2.0.0 ──
    ✔ forcats   1.0.0     ✔ readr     2.1.5
    ✔ ggplot2   3.4.4     ✔ stringr   1.5.1
    ✔ lubridate 1.9.3     ✔ tibble    3.2.1
    ✔ purrr     1.0.2     ✔ tidyr     1.3.1
    ── Conflicts ────────────────────────────────────────── tidyverse_conflicts() ──
    ✖ dplyr::filter() masks stats::filter()
    ✖ dplyr::lag()    masks stats::lag()
    ℹ Use the conflicted package (<http://conflicted.r-lib.org/>) to force all conflicts to become errors

``` r
library(lubridate)
```

``` r
connection <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
dbExecute(conn = connection, "INSTALL httpfs; LOAD httpfs;")
```

    [1] 0

``` r
PARQFILE = "https://storage.yandexcloud.net/arrow-datasets/tm_data.pqt"

Query <- "SELECT * FROM read_parquet([?])"
df <- dbGetQuery(connection, Query, list(PARQFILE))
```

### Шаг 2. Обработка данных

#### 1. Найдите утечку данных из Вашей сети

Важнейшие документы с результатами нашей исследовательской деятельности
в области создания вакцин скачиваются в виде больших заархивированных
дампов. Один из хостов в нашей сети используется для пересылки этой
информации – он пересылает гораздо больше информации на внешние ресурсы
в Интернете, чем остальные компьютеры нашей сети. Определите его
IP-адрес.

``` r
ipadd <- df  %>% 
  filter(!grepl('^(12|13|14).*', dst)) %>%
  group_by(src) %>% 
  summarise(bytes_amount = sum(bytes)) %>% 
  top_n(n = 1, wt = bytes_amount) %>%
  pull(src)
print(ipadd)
```

    [1] "13.37.84.125"

#### 2. Надите утечку данных 2

Другой атакующий установил автоматическую задачу в системном
планировщике cron для экспорта содержимого внутренней wiki системы. Эта
система генерирует большое количество трафика в нерабочие часы, больше
чем остальные хосты. Определите IP этой системы. Известно, что ее IP
адрес отличается от нарушителя из предыдущей задачи.

``` r
ipadd2 <- df %>%
  select(timestamp, src, dst, bytes) %>%
  mutate(timestamp = hour(as_datetime(timestamp/1000))) %>%
  filter(!grepl('^(12|13|14).*', dst) & timestamp >= 0 & timestamp <= 15 & src != "13.37.84.125") %>%
  group_by(src) %>%
  summarise(bytes_amount = sum(bytes)) %>%
  top_n(1, wt = bytes_amount)
print(ipadd2$src)
```

    [1] "12.55.77.96"

#### 3. Найдите утечку данных 3

Еще один нарушитель собирает содержимое электронной почты и отправляет в
Интернет используя порт, который обычно используется для другого типа
трафика. Атакующий пересылает большое количество информации используя
этот порт, которое нехарактерно для других хостов, использующих этот
номер порта. Определите IP этой системы. Известно, что ее IP адрес
отличается от нарушителей из предыдущих задач.

``` r
ipadd3 <- df %>%
  select(src, port, dst, bytes) %>%
  filter(!str_detect(dst, '^(12|13|14).')) %>%
  group_by(src, port) %>%
  summarise(bytes_ip_port = sum(bytes), .groups = "drop") %>%
  group_by(port) %>%
  mutate(average_port_traffic = mean(bytes_ip_port)) %>%
  ungroup() %>%
  top_n(1, bytes_ip_port / average_port_traffic)
print(ipadd3$src)
```

    [1] "12.30.96.87"

#### 4. Обнаружение канала управления

Зачастую в корпоротивных сетях находятся ранее зараженные системы,
компрометация которых осталась незамеченной. Такие системы генерируют
небольшое количество трафика для связи с панелью управления бот-сети, но
с одинаковыми параметрами – в данном случае с одинаковым номером порта.
Какой номер порта используется бот-панелью для управления ботами?

``` r
num_port<- df%>%
  group_by(port) %>%
  summarise(minBytes = min(bytes),
            maxBytes = max(bytes),
            diffBytes = max(bytes) - min(bytes),
            avgBytes = mean(bytes),
            count = n()) %>%
  filter(avgBytes - minBytes < 10 & minBytes != maxBytes) %>%
  select(port)
num_port
```

    # A tibble: 1 × 1
       port
      <int>
    1   124

## Вывод

В ходе работы с DuckDB и R изучены методы обработки больших данных,
анализ метаинформации о сетевом трафике и применение облачных технологий
для хранения и анализа данных. Полученные навыки полезны для
эффективного анализа информации в различных областях.
