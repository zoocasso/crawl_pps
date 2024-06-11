CREATE TABLE temp1
SELECT prdctClsfcNo, MAX(cntrctPrceAmt) AS high_cost
FROM `TB_PPS_MALL_LIST`
GROUP BY prdctClsfcNo;

CREATE TABLE temp2                                                  
SELECT a.*                                                          
FROM `TB_PPS_MALL_LIST` a                                             
INNER JOIN temp1 b                                                  
ON b.prdctClsfcNo = a.prdctClsfcNo AND b.high_cost = a.cntrctPrceAmt;

CREATE TABLE TB_PPS_MALL_LIST_HIGH_COST
SELECT *
FROM (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY prdctClsfcNo ORDER BY prdctIdntNo) AS a
	FROM temp2) AS b
WHERE a = 1;

ALTER TABLE TB_PPS_MALL_LIST_HIGH_COST DROP COLUMN a;

drop table temp1;
drop table temp2;