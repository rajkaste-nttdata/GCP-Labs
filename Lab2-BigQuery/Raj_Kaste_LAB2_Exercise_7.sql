SELECT l.name as language_name, Count(l.name) as count FROM `bigquery-public-data.github_repos.languages`,
UNNEST(language) as l
Group by language_name
having language_name = "JavaScript"