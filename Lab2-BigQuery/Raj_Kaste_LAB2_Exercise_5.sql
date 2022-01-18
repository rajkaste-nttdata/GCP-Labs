SELECT users.id AS id_user,
COUNT(pa.owner_user_id) as count
FROM `bigquery-public-data.stackoverflow.users` users
JOIN
`bigquery-public-data.stackoverflow.posts_answers` pa
ON users.id=pa.owner_user_id
group by id_user
ORDER BY count DESC
LIMIT 10