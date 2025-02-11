CREATE TABLE temp1
SELECT b.prdctClsfcNo, b.prdctIdntNo FROM (
SELECT
a.prdctClsfcNo,
a.prdctIdntNo,
row_number() over(partition BY a.prdctClsfcNo order BY a.prdctIdntNo) rn,
count(*) over(partition BY a.prdctClsfcNo) cnt
FROM `TB_PPS_MALL_LIST` a
) b
WHERE b.rn = ROUND(b.cnt/2);

CREATE TABLE temp2
SELECT b.*
FROM temp1 a
INNER JOIN `TB_PPS_MALL_LIST` b
ON a.prdctClsfcNo = b.prdctClsfcNo AND a.prdctIdntNo = b.prdctIdntNo;

CREATE TABLE TB_PPS_MALL_LIST_MEDIAN_COST
SELECT *
FROM (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY prdctClsfcNo ORDER BY prdctIdntNo) AS a
	FROM temp2) AS b
WHERE a = 1;

ALTER TABLE TB_PPS_MALL_LIST_MEDIAN_COST DROP COLUMN a;

drop table temp1;
drop table temp2;