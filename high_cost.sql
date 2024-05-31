CREATE TABLE temp1
SELECT prdctClsfcNo, prdctIdntNo, MAX(cntrctPrceAmt) AS high_cost
FROM `TB_PPS_MALL_LIST`
GROUP BY prdctClsfcNo, prdctIdntNo;

CREATE TABLE temp2
SELECT a.*
FROM temp1 a
INNER JOIN (
SELECT prdctClsfcNo, MAX(high_cost) AS 최대
FROM temp1
GROUP BY prdctClsfcNo
) b
ON a.prdctClsfcNo=b.prdctClsfcNo AND a.high_cost = b.최대;

CREATE TABLE temp3
SELECT prdctClsfcNo, MAX(prdctIdntNo) AS prdctIdntNo FROM temp2
GROUP BY prdctClsfcNo;

CREATE TABLE TB_PPS_MALL_LIST_HIGH_COST
SELECT b.* FROM temp3 a
LEFT JOIN `TB_PPS_MALL_LIST` b
ON a.prdctClsfcNo = b.prdctClsfcNo AND a.prdctIdntNo = b.prdctIdntNo;


drop table temp1;
drop table temp2;
drop table temp3;