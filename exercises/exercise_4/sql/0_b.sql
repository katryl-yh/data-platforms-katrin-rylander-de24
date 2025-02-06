-- b) Find out number of data engineer programs that got approved.

SELECT
    COUNT(*) AS DE_approved
FROM
    myh_2024
WHERE
    utbildningsnamn = 'Data Engineer' 
    AND beslut = 'Beviljad';