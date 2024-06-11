CREATE TABLE temp1
SELECT prdctClsfcNo, prdctIdntNo as prdctIdntNo, COUNT(*) AS 개수
FROM `TB_PPS_SHOPPING_DELIVERY`
GROUP BY prdctClsfcNo, prdctIdntNo;

CREATE TABLE temp2
SELECT *
FROM (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY prdctClsfcNo ORDER BY 개수 DESC) AS a
	FROM temp1) AS b
WHERE a = 1;

CREATE TABLE TB_PPS_SHOPPING_DELIVERY_MOST_SELLER
SELECT a.*
FROM `TB_PPS_API` a
INNER JOIN temp2 b
ON a.prdctClsfcNo = b.prdctClsfcNo
AND a.prdctIdntNo = b.prdctIdntNo;


drop table temp1;
drop table temp2;