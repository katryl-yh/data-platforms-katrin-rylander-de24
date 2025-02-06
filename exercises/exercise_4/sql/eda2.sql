SELECT
    utbildningsnamn,
    utbildningsområde,
    beslut,
    "utbildningsanordnare administrativ enhet",
    kommun
FROM
    myh_2024
WHERE
    beslut = 'Beviljad'
    AND  "utbildningsanordnare administrativ enhet" = 'Stiftelsen Stockholms Tekniska Institut';