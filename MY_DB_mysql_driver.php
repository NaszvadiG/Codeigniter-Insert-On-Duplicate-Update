<?php

/**
 * Class MY_DB_mysql_driver
 * @author Adebola Samuel Olowofela
 * I improved this code which was placed on the CI forum, this is me giving credit to whoever started the thread
 */
class MY_DB_mysql_driver extends CI_DB_mysql_driver {

    /**
     * This will represent the number of key=>value pairs that would be batch inserted once
     * @staticvar int
     */
    const NUMBER_PER_BATCH_INSERT  = 150;
    /**
     * @staticvar string the lang label that holds the message for db_must_set_table
     */
    const MESSAGE_NO_TABLE = "db_must_set_table";
    /**
     * @staticvar string the lang label that holds the message for db_must_use_set
     */
    const MESSAGE_MUST_USE_SET = "db_must_use_set";

    /**
     * This will initiate the constructor for the mysql driver
     * @param $params
     */
    final public function __construct($params)
    {
        parent::__construct($params);
        log_message('debug', 'Extended DB driver class instantiated!');
    }

    /**
     * Insert_On_Duplicate_Update_Batch
     *
     * This method filters and performs operation to insert a set of key=>value pairs into a table
     * NOTE: It would insert a record if it is not available, however, it would update the record if
     * it already exists
     *
     * This is performed using MySQL 'ON DUPLICATE KEY UPDATE'
     *
     * @access public
     * @author Adebola
     * @param string $sTable The name of the table to insert/update into
     * @param null|array $aData This is an array of key => value pairs to insert
     * @param array $aExtraUpdateFields - This is a key => value pair of items to be appended to the update query
     * It can be useful in setting modified_time etc
     * @param array $aExcludeFromUpdate - This is an array of keys that represents the columns to exclude from the update
     * This can be useful in removing columns like created_time from the update query
     * @return object
     */
    function insert_on_duplicate_update_batch($sTable = '',
                                              $aData = NULL,
                                              $aExtraUpdateFields = array(),
                                              $aExcludeFromUpdate = array())
    {
        // check if the table is available
        if (empty($sTable))
        {
            // set to existing from table if available
            if ( ! isset($this->ar_from[0]))
            {
                if ($this->db_debug)
                {
                    return $this->display_error(self::MESSAGE_NO_TABLE);
                }
                return FALSE;
            }

            $sTable = $this->ar_from[0];
        }

        // check if the data is prepared and available
        if ( ! is_null($aData))
        {
            $this->set_insert_batch($aData);
        }

        if (count($this->ar_set) == 0)
        {
            if ($this->db_debug)
            {
                //No valid data array.  Folds in cases where keys and values did not match up
                return $this->display_error(self::MESSAGE_MUST_USE_SET);
            }
            return FALSE;
        }

        // perform the batch operation
        for ($i = 0, $iTotalData = count($this->ar_set); $i < $iTotalData; $i = $i + self::NUMBER_PER_BATCH_INSERT)
        {

            $sQuery = $this->_insert_on_duplicate_update_batch(
                $this->_protect_identifiers($sTable, TRUE, NULL, FALSE),
                $this->ar_keys,
                array_slice($this->ar_set, $i, self::NUMBER_PER_BATCH_INSERT),
                $aExtraUpdateFields,
                $aExcludeFromUpdate
            );

            // perform teh SQL query
            $this->query($sQuery);
        }

        // reset the write pointer
        $this->_reset_write();


        return TRUE;
    }

    /**
     * Insert_on_duplicate_update_batch statement
     *
     * @access public
     * @param string the table name
     * @param array the insert keys
     * @param array the insert values
     * @param array $aExtraUpdateFields - This is a list of extra fields to add in case of updates
     * @param array $aExcludeFromUpdate - This is a list of extra fields to add in case of updates
     * @return string
     */
    private function _insert_on_duplicate_update_batch($sTable, $aKeys, $aValues,
                                                       $aExtraUpdateFields = array(),
                                                       $aExcludeFromUpdate = array())
    {
        // create the update fields
        $aUpdateFields = array();

        if(!empty($aExcludeFromUpdate))
            $aExcludeFromUpdate = $this->prepareFieldsFromArray($aExcludeFromUpdate);


        foreach($aKeys as $key)
        {
            if(!in_array($key, $aExcludeFromUpdate))
                $aUpdateFields[] = $key.'=VALUES('.$key.')';
        }


        // include the extra update fields if it is available
        if(!empty($aExtraUpdateFields) && is_array($aExtraUpdateFields)){
            foreach($aExtraUpdateFields as $sKey=>$sValue)
                $sKey = $this->_escape_identifiers($sKey);
                $sValue = $this->escape($sValue);
                $aUpdateFields[] = $sKey."= $sValue";
        }

        // build the field names
        $sFieldNames     = implode(",", $aKeys);
        // build the value string
        $sValueString   = implode(",", $aValues);
        // build the update string
        $sUpdateString  = implode(', ', $aUpdateFields);


        // build the query here
        $sQuery = "INSERT INTO {$sTable} ({$sFieldNames}) ";
        $sQuery .= " VALUES {$sValueString} ON DUPLICATE KEY UPDATE {$sUpdateString}";

        return $sQuery;
    }


    /**
     * This method prepares the fields from the array supplied
     * Structure of array should be as follows
     * $aArray = array(
     *  'FieldName1',
     *  'FieldName2'
     *  'FieldName3'
     *   ...
     *  'FieldNameN'
     * );
     * @param array $aFieldsArray
     * @return array
     * @author Adebola
     */
    private function prepareFieldsFromArray($aFieldsArray = array()){

        $aReturnArray = array();
        if(!empty($aFieldsArray) && is_array($aFieldsArray)){
            foreach($aFieldsArray as $mValue){
                if(!empty($mValue) && is_string($mValue) && !in_array($mValue, $aReturnArray))
                    $aReturnArray[] = $this->_escape_identifiers($mValue);
            }

        }

        return $aReturnArray;
    }

}
