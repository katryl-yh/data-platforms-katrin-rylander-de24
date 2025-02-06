-- a) Find out how many data engineer programs have been applied
-- and which schools have applied them along

SELECT
    utbildningsnamn,
    utbildningsområde,
    "utbildningsanordnare administrativ enhet"
FROM
    myh_2024
WHERE
    utbildningsnamn = 'Data Engineer';