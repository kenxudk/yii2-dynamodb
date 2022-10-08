<?php
namespace src;
use Yii;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use Aws\DynamoDb\DynamoDbClient;
use Aws\Sdk;
class Dynamodb
{
    protected static $_client;
    protected static $_marshaler;
    protected static $item;
    protected static $connection;
    protected static $table_prefix;
    private function __construct()
    {
    }
    private function __clone()
    {
    }

    /**
     * new aws dynamodb客服端
     * @return DynamoDbClient
     * User: ken xu
     * Date: 2022-10-08 18:59
     */
    public function client()
    {
        if (self::$_client instanceof self) {
            return self::$_client;
        }
        $key  =    Yii::$app->params["aws"]["key"];
        $secret  = Yii::$app->params["aws"]["secret"];
        $region  = Yii::$app->params["aws"]["region"];
        $version = Yii::$app->params["aws"]["version"];
        return  new DynamoDbClient([
            // 'endpoint'   => 'http://localhost:8000',
            'region'   => $region,
            'version'  => $version,
            'credentials' => [
                'key' => $key,
                'secret' => $secret,
            ],
        ]);
    }

    /**
     * new aws的 Marshaler client
     * @return Marshaler
     * User: ken xu
     * Date: 2022-10-08 19:00
     */
    public  function Marshal()
    {
        if (self::$_marshaler !== null)
            return self::$_marshaler;

        return self::$_marshaler = new Marshaler();
    }

    /**
     * 数组转化为dynamodb的格式
     * @param array $array
     * @return array
     * User: ken xu
     * Date: 2022-10-08 18:59
     */
    public  function marshalItem($array=[]){
        return $this->Marshal()->marshalItem($array);
    }

    /**
     * dynamodb转化为数组的格式
     * @param array $array
     * @return array|\stdClass
     * User: ken xu
     * Date: 2022-10-08 19:00
     */
    public  function unmarshalItem($array=[]){
         return $this->Marshal()->unmarshalItem($array);
    }


    public static function getConnection()
    {
        if (self::$connection !== null)
            return self::$connection;

        return self::$connection = new self();
    }

    /**
     * @param string $table_name  表名
     * @param string $IndexName   全局索引名
     * @return bool|Dynamodb
     * User: ken xu
     * Date: 2022-10-08 19:01
     */
    public  static function table($table_name='',$IndexName=''){
        if(empty($table_name)){
            return false;
        }
        self::$item = [];
        self::$item['TableName'] = $table_name;
        if(!empty($IndexName)){
            //二级索引名
            self::$item['IndexName'] = $IndexName;
        }
        return self::getConnection();
    }

    /**
     * where 条件拼接
     * 如果为string则为queryitem条件样式，数组这位主键条件
     * 在queryitem条件样式时expressValue()必填
     * document: https://docs.aws.amazon.com/aws-sdk-php/v3/api/api-dynamodb-2012-08-10.html#query
     * @param $where
     * @return Dynamodb
     * User: ken xu
     * Date: 2022-10-08 19:02
     */
    public function kWhere($where){
        if(is_array($where)){
            if(count($where) == count($where,1)){
                self::$item['Key'] = $this->marshalItem($where);
            }else{
                foreach ($where as $key => $value) {
                    $where[$key] = $this->marshalItem($value);
                }
                self::$item['Keys'] = $where;
            }
            
        }else if(is_string($where)){
            self::$item['KeyConditionExpression'] = $where;
        }
        return self::getConnection();
    }

    /**
     * 拼入 FilterExpression ，过滤条件
     * @param $str
     * @return Dynamodb
     * User: ken xu
     * Date: 2022-10-08 19:06
     */
    public function fWhere($str){
        self::$item['FilterExpression'] = $str;
        return self::getConnection();
    }

    /**
     * 拼入conditionExpression在删除时非key值条件用这个
     * @param $str
     * @return Dynamodb
     * User: ken xu
     * Date: 2022-10-08 19:07
     */
    public function cWhere($str){
        self::$item['ConditionExpression'] = $str;
        return self::getConnection();
    }


    /**
     * 拼入 ExpressionAttributeValues
     * @param array $array  key作为占位字符,以:开头，value作为值 . example: [':v1'=>'string',':v2'=>11,...]
     * @return Dynamodb
     * User: ken xu
     * Date: 2022-10-08 19:07
     */
    public function expressValue($array=[]){
        self::$item['ExpressionAttributeValues'] = $this->marshalItem($array);
        return self::getConnection();
    }


    /**
     * 在名字与dbnamodb保留关键词冲突是使用
     * @param array $array  example: ['#A'=>'filename','#A2'=>'filename2',...]
     * @return Dynamodb
     * User: ken xu
     * Date: 2022-10-08 19:08
     */
    public function  expressName($array=[]){
        self::$item['ExpressionAttributeNames'] = $array;
        return self::getConnection();
    }

    /**
     * 排序，这里默认为false，排序主键 倒叙
     * @param bool $order
     * @return Dynamodb
     * User: ken xu
     * Date: 2022-10-08 19:08
     */
    public function order($order=false){
        self::$item['ScanIndexForward'] = $order;
        return self::getConnection();
    }


    /**
     * 是否强制一致性，默认不
     * @param bool $is_force
     * @return Dynamodb
     * User: ken xu
     * Date: 2022-10-08 19:09
     */
    public function force($is_force=false){
        self::$item['ConsistentRead'] = $is_force;
        return self::getConnection();
    }


    /**
     * 对结果再次筛选. "FilterExpression": "delstatus = :v3",
     * @param string $str
     * @return Dynamodb
     * User: ken xu
     * Date: 2022-10-08 19:09
     */
   public function having($str=''){
        self::$item['FilterExpression'] =  $str;
        return self::getConnection();
   }


    /**
     * 限制条数
     * @param int $num
     * @param array $lastEvaluatedKey  如果有值，下次query从这离开时
     * @return Dynamodb
     * User: ken xu
     * Date: 2022-10-08 19:09
     */
   public function limit($num=10,$lastEvaluatedKey=[]){
        if(intval($num) > 0){
            self::$item['Limit'] = $num;
        }
        if(!empty($lastEvaluatedKey) && is_array($lastEvaluatedKey)){
            self::$item['ExclusiveStartKey'] = $this->marshalItem($lastEvaluatedKey);
        }
        return self::getConnection();
   }

    /**
     * 要查询字段
     * @param string $str
     * @return Dynamodb
     * User: ken xu
     * Date: 2022-10-08 19:10
     */
   public function select($str=''){
       if($str){
           self::$item['ProjectionExpression'] = $str;
       }
       return self::getConnection();
   }

    /**
     * 数据单个插入
     * @param array $array
     * @return bool
     * User: ken xu
     * Date: 2022-10-08 19:11
     */
    public function insert($array=[]){
         try {
            self::$item['Item'] = $this->marshalItem($array);
            $this->client()->putItem(self::$item);
            return true;
        } catch (DynamoDbException $e) {
            self::log($e->getMessage());
            return false;
        }
    }

    /**
     * 插入多个
     * @param array $arrays
     * @return bool
     * User: ken xu
     * Date: 2022-10-08 19:13
     */
    public function inserts($arrays=[]){
       try {
            $items = [];
            foreach ($arrays as $key => $value) {
                $value = $this->marshalItem($value);
                $items[]['PutRequest']['Item'] = $value;
            }
            $tablename = self::$item['TableName'];
            unset(self::$item['TableName']);
            $this->client()->batchWriteItem([
                'RequestItems' => [ $tablename => $items ],
            ]);
            return true;
        }catch (DynamoDbException $e) {
           self::log($e->getMessage());
           return false;
        }
    }

    /**
     * 删除
     * @param array $arrays
     * @return bool
     * User: ken xu
     * Date: 2022-10-08 19:13
     */
    public function deletes($arrays=[]){
       try {
            $items = [];
            foreach ($arrays as $key => $value) {
                $value = $this->marshalItem($value);
                $items[]['DeleteRequest']['Key'] = $value;
            }
            $tableName = self::$item['TableName'];
            unset(self::$item['TableName']);
            $this->client()->batchWriteItem([
                'RequestItems' => [ $tableName => $items ],
            ]);
            return true;
        }catch (DynamoDbException $e) {
           self::log($e->getMessage());
           return false;
        }
    }

    /**
     * 根据区键与排序键单个查询
     * @return array|bool|mixed|\stdClass|null
     * User: ken xu
     * Date: 2022-10-08 19:14
     */
    public function one(){
        try {
            $result = $this->client()->getItem(self::$item);
            if(is_array($result['Item'])){
                return $this->unmarshalItem($result['Item']);
            }
            return $result['Item'];
        } catch (DynamoDbException $e) {
            self::log($e->getMessage());
            return false;
        }
    }

    /**
     * 条件查询
     * @return array|bool
     * User: ken xu
     * Date: 2022-10-08 19:14
     */
    public function query(){
        try {
            $result = $this->client()->query(self::$item);
            $returnData = [];
            if(is_array($result['Items']) && !empty($result['Items'])){
                $returnData['data'] = [];
                foreach ($result['Items'] as $key => $value) {
                    $value = $this->unmarshalItem($value);
                    $returnData['data'][] = $value;
                }
                $returnData['lastEvaluatedKey'] = isset($result['LastEvaluatedKey']) ? $this->unmarshalItem($result['LastEvaluatedKey']) : [];
                return $returnData;
            }
            return $returnData;
        } catch (DynamoDbException $e) {
            self::log($e->getMessage());
            return false;
        }
    }
    /**
     * 基于区键与排序键多个查询
     * @return array|\Aws\Result|bool
     * User: ken xu
     * Date: 2022-10-08 19:15
     */
    public function ones(){
        try {
            $tableName = self::$item['TableName'];
            unset(self::$item['TableName']);
            $result = $this->client()->batchGetItem([
                'RequestItems' => [$tableName => self::$item],
            ]);
            $result = $result['Responses'][$tableName];
            if(is_array($result)){
                foreach ($result as $key => $value) {
                    $value = $this->unmarshalItem($value);
                    $result[$key] = $value;
                }
            }
            return $result;
        }catch (DynamoDbException $e) {
            self::log($e->getMessage());
            return false;
        }
    }



    /**
     * 修改item
     * 待修改字段名
     * value修改的展位符名
     * @param array $array   example: [['createat',':v1'],['createat',':v2',-]]
     * @return \Aws\Result|bool
     * User: ken xu
     * Date: 2022-10-08 19:16
     */
    public function update($array=[]){
        $str = '';
        if(!empty($array)){
            $str = 'SET ';
            if(count($array) == count($array,1)){
                 $str .= $this->getStr($array);
            }else{
                foreach ($array as  $value) {
                    $str .= $this->getStr($value);
                }                
            }
            $str = trim($str,',');
        }
        try {
            self::$item['UpdateExpression'] = $str;
            $result = $this->client()->updateItem(self::$item);
            return $result;

        } catch (DynamoDbException $e) {
            self::log($e->getMessage());
            return false;
        }
    }
    
    public function getStr($value=[]){
        $str = '';
        if(isset($value[2])){
            $str .= ' '.$value[0].'='.$value[0].$value[2].$value[1].' ,';
        }else{
            $str .= ' '.$value[0].'='.$value[1].' ,';
        }
        return $str;
    }

    /**
     * 删除
     * @return \Aws\Result|bool
     * User: ken xu
     * Date: 2022-10-08 19:17
     */
    public function delete(){
         try {
            $result = $this->client()->deleteItem(self::$item);
            return $result;

        } catch (DynamoDbException $e) {
             self::log($e->getMessage());
             return false;
        }
    }

    /**
     * 全表扫描(谨慎使用，查询全部数据，很花钱)
     * @return bool
     * User: ken xu
     * Date: 2022-10-08 19:18
     */
    public  function scan()
    {
        try {
            $result = self::GetClient()->scan(self::$item);
            return $result['Items'];

        } catch (DynamoDbException $e) {
            self::log($e->getMessage());
            return false;
        }
    }

    /**
     * 日志记录
     * @param string $message
     * User: ken xu
     * Date: 2022-10-08 19:12
     */
    public static function log($message=''){
        \Yii::error(json_encode(self::$item).':'.$message,'kuDynamodb');
    }

}