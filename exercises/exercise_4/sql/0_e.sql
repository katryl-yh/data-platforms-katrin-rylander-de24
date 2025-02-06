-- e) Calculate the percentage of approved programs within each education category.

WITH
    approved AS (
        SELECT
            utbildningsområde,
            COUNT(*) AS approved_count
        FROM
            myh_2024
        WHERE
            beslut = 'Beviljad'
        GROUP BY utbildningsområde
    ),
    total AS (
        SELECT
            utbildningsområde,
            COUNT(*) AS total_count
        FROM
            myh_2024
        GROUP BY utbildningsområde
    )
SELECT
    approved.utbildningsområde,
    approved.approved_count,
    total.total_count,
    approved.approved_count *100 /  total.total_count AS approved_percentage
FROM
    approved
INNER JOIN total 
ON approved.utbildningsområde = total.utbildningsområde
ORDER BY approved_percentage