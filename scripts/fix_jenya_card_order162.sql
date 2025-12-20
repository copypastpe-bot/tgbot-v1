-- Исправление записи в реестре "Карта Жени" для заказа №162
-- Уменьшает запись на 5000₽ (корректировка разделения оплат)

-- Создаем корректирующую запись (expense) на 5000₽
INSERT INTO jenya_card_entries(kind, amount, comment, happened_at, created_at)
VALUES ('expense', 5000.00, 'Корректировка заказа №162 (разделение оплат)', NOW(), NOW());

-- Проверяем новый баланс
SELECT 
    COALESCE(SUM(CASE WHEN kind IN ('income','opening_balance') THEN amount ELSE 0 END),0) AS income_sum,
    COALESCE(SUM(CASE WHEN kind='expense' THEN amount ELSE 0 END),0) AS expense_sum,
    COALESCE(SUM(CASE WHEN kind IN ('income','opening_balance') THEN amount ELSE 0 END),0) - 
    COALESCE(SUM(CASE WHEN kind='expense' THEN amount ELSE 0 END),0) AS balance
FROM jenya_card_entries
WHERE COALESCE(is_deleted,false)=FALSE;

