DELIMITER //

CREATE PROCEDURE check_status()
BEGIN
    -- Select status for all records and return 1 for 'pending', 0 otherwise
    SELECT 
        id, 
        CASE 
            WHEN status = 'pending' THEN 1
            ELSE 0
        END AS status
    FROM 
        table_for_archival;
END //

DELIMITER ;
