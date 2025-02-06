--  d) Count nubmer of approved programs for each municipality (kommun).

SELECT
    kommun,
    COUNT(*) AS total_approved
FROM
    myh_2024
WHERE
    beslut = 'Beviljad'
GROUP BY kommun;