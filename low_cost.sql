CREATE TABLE temp1
SELECT prdctClsfcNo, prdctIdntNo, MIN(cntrctPrceAmt) AS low_cost
FROM `TB_PPS_MALL_LIST`
GROUP BY prdctClsfcNo, prdctIdntNo;

CREATE TABLE temp2 
SELECT a.*
FROM temp1 a
INNER JOIN (
SELECT prdctClsfcNo, MIN(low_cost) AS 최소
FROM temp1
GROUP BY prdctClsfcNo
) b
ON a.prdctClsfcNo=b.prdctClsfcNo AND a.low_cost = b.최소;

CREATE TABLE temp3
SELECT prdctClsfcNo, MAX(prdctIdntNo) AS prdctIdntNo FROM temp2
GROUP BY prdctClsfcNo;

CREATE TABLE TB_PPS_MALL_LIST_LOW_COST
SELECT b.* FROM temp3 a
LEFT JOIN `TB_PPS_MALL_LIST` b
ON a.prdctClsfcNo = b.prdctClsfcNo AND a.prdctIdntNo = b.prdctIdntNo;


drop table temp1;
drop table temp2;
drop table temp3;