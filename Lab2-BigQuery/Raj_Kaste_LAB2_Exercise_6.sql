SELECT P.owner_user_id AS id_user,
COUNT(S.owner_user_id) as count
FROM `bigquery-public-data.stackoverflow.stackoverflow_posts` S
JOIN `bigquery-public-data.stackoverflow.posts_answers` P
ON S.owner_user_id=P.owner_user_id
WHERE S.accepted_answer_id IS NOT NULL
GROUP BY P.owner_user_id,S.creation_date
HAVING EXTRACT (YEAR FROM S.creation_date)=2010
ORDER BY count DESC 
LIMIT 10