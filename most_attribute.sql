CREATE TABLE temp1
SELECT PRDCTCLSFCNO,PRDCTIDNTNO,COUNT(*) AS 개수
FROM `TB_PPS_INDIVIDUAL_ATTRIBUTE_INFORMATION`
GROUP BY PRDCTCLSFCNO, PRDCTIDNTNO;

CREATE TABLE temp2
SELECT *
FROM (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY prdctClsfcNo ORDER BY 개수 DESC) AS a
	FROM temp1) AS b
WHERE a = 1;

CREATE TABLE TB_PPS_MOST_INDIVIDUAL_ATTRIBUTE_INFORMATION
SELECT a.*                                                          
FROM `TB_PPS_API` a                                             
INNER JOIN temp2 b                                                 
ON a.prdctClsfcNo = b.PRDCTCLSFCNO
AND a.prdctIdntNo = b.PRDCTIDNTNO;


drop table temp1;
drop table temp2;