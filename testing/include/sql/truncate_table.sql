DELIMITER $$

CREATE PROCEDURE truncate_table(IN table_name VARCHAR(255))
BEGIN
    SET @query = CONCAT('TRUNCATE TABLE ', table_name);
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

DELIMITER ;
