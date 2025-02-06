-- c) Count number of approved programs in each of the education categories (utbildningsområde)

SELECT
    utbildningsområde,
    COUNT(*) AS total_approved
FROM
    myh_2024
WHERE
    beslut = 'Beviljad'
GROUP BY utbildningsområde;