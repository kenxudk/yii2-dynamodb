<a href="https://996.icu"><img src="https://img.shields.io/badge/link-996.icu-red.svg" alt="996.icu" /></a>
# xdk-dynamodb
 a simple dynamodb operation wrapper class（一个简单的dynamodb操作封装类）
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
 
 ### add
    $result = Dynamodb::table($table_name)->insert($data);
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
     $result = KmDynamodb::table($table_name)->expressValue($expressValue)->kwhere(['fid'=>$data['fid']])->expressName($expressName)->update($updata);

### select
#### Get multiples with the primary key (batchGetItem)
        $keys = [];
        foreach ($feed_id_list as $feed_id) {
            $keys[] = ['fid' => $feed_id];
        }
        $feeds = Dynamodb::table(self::$feed_name)->kwhere($keys)->order(true)->ones(); 
####    Get one with the primary key (getItem)
       Dynamodb::table(self::$like_name)->kwhere(['lid'=>$lid])->one();
#### (query)
     Dynamodb::table(self::$comment_name,self::GSI_FEED)->kwhere('fid=:v')
     ->expressValue([':v'=>$feed_id])->limit(0,$last)->query();    

### delete
    Dynamodb::table(self::$feed_name)->kwhere(['fid'=>$data['fid']])->delete();  
