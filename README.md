<a href="https://996.icu"><img src="https://img.shields.io/badge/link-996.icu-red.svg" alt="996.icu" /></a>
# yii2-dynamodb
 a simple dynamodb operation wrapper class
 （一个简单的aws dynamodb操作封装类）
# description

 Because I used the YII2 framework, the log is yii, and the AWS package is placed under the vender. The configuration file for this class is in params.php. as follows:
 
 因为我用的时YII2框架，所以日志用的是yii的，并且AWS包放在vender下面，这个类的配置文件在params.php里面,如下：

	   'aws'=>[
	        'region'=>'ap-southeast-1',
	        'version'=>'',
	        'key'=>"",
	        'secret'=>"",
	    ],
 # simple use
 
 TABLE_NAME is the name of the table you are using, TABLE_INDEX is currently using the index name 
（说明TABLE_NAME是你使用表名，TABLE_INDEX当前使用的索引名称）
 ### add
    $result = Dynamodb::table(TABLE_NAME)->insert($data);
 ### update
    $expressValue = $updata = $expressName = [];
    $i = 0;
    foreach ($up_arr as $key => $value) {
        $f = '#f'.$i;
        $v = ':v'.$i;
        $expressName[$f] = $key;
        $updata[] = [$f,$v];
        $expressValue[$v] = $value;
        $i++;
    }
     $result = KmDynamodb::table(TABLE_NAME)->expressValue($expressValue)->kWhere(['pk_id'=>$primary_key])->expressName($expressName)->update($updata);

### select
#### Get multiples with the primary key (batchGetItem)
        $keys = [];
        foreach ($primary_keys as $primary_key) {
            $keys[] = ['kid' => $primary_key];
        }
        $feeds = Dynamodb::table(TABLE_NAME)->kWhere($keys)->order(true)->ones(); 
#### Get one with the primary key (getItem)
       Dynamodb::table(TABLE_NAME)->kWhere(['kid'=>$primary_key])->one();
#### query
     Dynamodb::table(TABLE_NAME,TABLE_INDEX)->kWhere('kid=:v')
     ->expressValue([':v'=>$primary_key])->limit(10,[])->query();    

### delete
    Dynamodb::table(TABLE_NAME)->kWhere(['kid'=>$primary_key])->delete();  
