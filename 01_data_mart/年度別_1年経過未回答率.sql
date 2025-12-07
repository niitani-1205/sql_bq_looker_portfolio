-- ============================================
-- StackOverflow Q&A データマート
-- 年度別：投稿から1年（365日）経過しても回答がつかなかった割合
-- WITH句(1)~(4)は年度別回答速度と同じデータマートを使用
-- ============================================

-- 1.質問データの前処理
WITH questions AS (
  SELECT
    id                              AS question_id,         -- 質問ID
    EXTRACT(YEAR FROM creation_date)AS question_year,       -- 質問が投稿された年
    creation_date                   AS question_date        -- 質問日
  FROM 
    `bigquery-public-data.stackoverflow.posts_questions`
),

-- 2.回答データの前処理
answers AS (
  SELECT
    parent_id                       AS question_id,         -- 紐づく質問ID
    creation_date                   AS answer_date          -- 回答日
  FROM 
    `bigquery-public-data.stackoverflow.posts_answers`
),

-- 3.WITH句1と2を結合
-- (各質問ごとに最初に回答された日時を算出する)
-- (回答が無い質問も残すため LEFT JOIN を使用)
first_answer AS (
  SELECT
    q.question_id                   AS question_id,         -- 質問ID
    q.question_year                 AS question_year,       -- 質問年
    q.question_date                 AS question_date,       -- 質問日
    MIN(a.answer_date)              AS first_answer_date,   -- 最初の回答日時（NULL可）
    COUNT(a.answer_date)            AS total_answers        -- 回答数
  FROM 
    questions AS q
  LEFT JOIN 
    answers AS a
    ON q.question_id = a.question_id
  GROUP BY
    q.question_id, 
    q.question_year, 
    q.question_date
),


-- 4.データマート化
-- (質問1件につき1行の集約形式に整備)
-- (「未回答フラグ」「回答時間（分）」を付与した分析用中間テーブルを作成)
answer_mart AS (
  SELECT
    question_id,                       -- 質問ID
    question_year,                     -- 質問年
    question_date,                     -- 質問日
    first_answer_date,                 -- 最初の回答日時
    total_answers,                     -- 回答数
    IF(first_answer_date IS NULL, 1, 0)
                                      AS flg_unanswered,       -- 未回答フラグ
    IF(first_answer_date IS NULL, NULL,
       TIMESTAMP_DIFF(first_answer_date, question_date, MINUTE)
    )                                 AS minutes_to_answer     -- 回答時間（分）
  FROM first_answer -- WITH句(3)
)


-- 5.最終出力：1年経っても回答されなかった質問の割合(年度別)
SELECT
  question_year,                                                        -- 質問年度

  -- 1年経っても回答されなかった質問の割合
  ROUND(  
    AVG(
      CASE
        WHEN minutes_to_answer IS NULL             -- そもそも回答なし（未回答）
          OR minutes_to_answer > 60*24*365         -- 回答はあったが1年超遅れ
        THEN 1 ELSE 0 
      END
    ),
    4
  )                                              AS unanswered_over_1y   -- 1年超未解決率

FROM 
  answer_mart -- WITH句(4)
GROUP BY
  question_year
ORDER BY
  question_year
;
