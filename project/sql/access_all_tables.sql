DELIMITER //

CREATE PROCEDURE GetTableRecords(IN tableName VARCHAR(255))
BEGIN
    SET @query = CONCAT('SELECT * FROM ', tableName);
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;
