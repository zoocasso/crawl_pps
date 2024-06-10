CREATE TABLE temp1
SELECT prdctClsfcNo, prdctIdntNo as prdctIdntNo, COUNT(*) AS 개수
FROM `TB_PPS_SHOPPING_DELIVERY`
GROUP BY prdctClsfcNo, prdctIdntNo;

CREATE TABLE temp2
SELECT a.*
FROM temp1 a
INNER JOIN (
SELECT prdctClsfcNo, MAX(개수) AS 최대개수
FROM temp1
GROUP BY prdctClsfcNo
) b
ON a.prdctClsfcNo=b.prdctClsfcNo and a.개수 = b.최대개수;

CREATE TABLE temp3 
SELECT prdctClsfcNo, MAX(prdctIdntNo) AS prdctIdntNo
FROM temp2
GROUP BY prdctClsfcNo;

CREATE TABLE TB_PPS_SHOPPING_DELIVERY_MOST_SELLER
SELECT b.*
FROM temp3 a
INNER JOIN `TB_PPS_MALL_LIST` b
ON a.prdctClsfcNo = b.prdctClsfcNo
AND a.prdctIdntNo = b.prdctIdntNo;


drop table temp1;
drop table temp2;
drop table temp3;