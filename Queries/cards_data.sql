-- id
SELECT *
FROM ingestion.cards_data
ORDER BY id ASC;

SELECT MIN(id) AS smallest_id, MAX(id) AS largest_id
FROM ingestion.cards_data

SELECT TOP 1 
    id, 
    LEN(id) AS character_count
FROM ingestion.cards_data
ORDER BY LEN(id) DESC;

SELECT *
FROM ingestion.cards_data
WHERE id = '' OR id IS NULL

SELECT id, COUNT(id) AS id_count
FROM ingestion.cards_data
GROUP BY id
HAVING COUNT(id) > 1;

-- client_id
SELECT *
FROM ingestion.cards_data
ORDER BY client_id ASC;

SELECT MIN(client_id) AS smallest_id, MAX(client_id) AS largest_id
FROM ingestion.cards_data

SELECT TOP 1 
    client_id, 
    LEN(client_id) AS character_count
FROM ingestion.cards_data
ORDER BY LEN(client_id) DESC;

SELECT *
FROM ingestion.cards_data
WHERE client_id = '' OR client_id IS NULL

-- card_brand
SELECT Distinct(card_brand)
FROM ingestion.cards_data 

SELECT *
FROM ingestion.cards_data
WHERE card_brand = '' OR card_brand IS NULL

-- card_type
SELECT Distinct(card_type)
FROM ingestion.cards_data 

-- card_number
SELECT TOP 1 
    card_number, 
    LEN(card_number) AS character_count
FROM ingestion.cards_data
ORDER BY LEN(card_number) DESC;

SELECT *
FROM ingestion.cards_data
WHERE card_number = '' OR card_number IS NULL

-- expires
SELECT TOP 1 
    expires, 
    LEN(expires) AS character_count
FROM ingestion.cards_data
ORDER BY LEN(expires) DESC;

SELECT *
FROM ingestion.cards_data
WHERE expires = '' OR expires IS NULL

-- cvv
SELECT TOP 1 
    cvv, 
    LEN(cvv) AS character_count
FROM ingestion.cards_data
ORDER BY LEN(cvv) DESC;

SELECT *
FROM ingestion.cards_data
WHERE cvv = '' OR cvv IS NULL

-- has_chip
SELECT Distinct(has_chip)
FROM ingestion.cards_data 

-- num_cards_issued
SELECT Distinct(num_cards_issued)
FROM ingestion.cards_data 

-- credit_limit
SELECT Distinct(credit_limit)
FROM ingestion.cards_data 

SELECT *
FROM ingestion.cards_data
WHERE credit_limit = '' OR credit_limit IS NULL

-- acct_open_date
SELECT *
FROM ingestion.cards_data
WHERE acct_open_date = '' OR acct_open_date IS NULL

-- year_pin_last_changed
SELECT *
FROM ingestion.cards_data
WHERE year_pin_last_changed = '' OR year_pin_last_changed IS NULL

-- card_on_dark_web
SELECT Distinct(card_on_dark_web)
FROM ingestion.cards_data 

-- issuer_bank_name
SELECT Distinct(issuer_bank_name)
FROM ingestion.cards_data 

-- issuer_bank_state
SELECT Distinct(issuer_bank_state)
FROM ingestion.cards_data 

-- issuer_bank_type
SELECT Distinct(issuer_bank_type)
FROM ingestion.cards_data 

-- issuer_risk_rating
SELECT Distinct(issuer_risk_rating)
FROM ingestion.cards_data 

-- miscelanious
SELECT expires, acct_open_date
FROM ingestion.cards_data