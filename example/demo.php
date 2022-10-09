<?php
use src\Dynamodb;
class demo{
    /*简单的点赞表例子
     * Like:
        {
          "lid": {
            "S": "5089#sounddk1up9i3as7tbc45bbfr0u82td#1"
          },
          "avatar": {
            "S": "/avatar/sounddk1up9i3as7tbc45bbfr0u82td-16288441826181391218617_80.webp"
          },
          "created": {
            "N": "1628844611"
          },
          "fid": {
            "S": "5089#1"
          },
          "gender": {
            "N": "0"
          },
          "name": {
            "S": "bruce8989"
          },
          "uid": {
            "S": "sounddk1up9i3as7tbc45bbfr0u82td"
          }
        }
     */
    const LIKE_TABLE_NAME = 'like';
    const GSI_FID = 'gsi_feed';//like的一个全局索引， fid为pk,created 为sk
    /**
     * 添加一个点赞
     * User: ken xu
     * Date: 2022-10-09 10:40
     */
    public function add(){
        $user_id = '1up9i3as7tbc45bbfr0u82td'.random_int(1000,9999);
        $data = [
            'lid'     => '5089#'.$user_id.'#1',//primary key
            'avatar'  => '/avatar/sounddk1up9i3as7tbc45bbfr0u82td-16288441826181391218617_80.webp',
            'created' => time(),
            'fid'     => '5089#1',
            'gender'  => 0,
            'name'    => 'demo'.random_int(1000,9999),
            'uid'     => $user_id,
        ];
        $result =  Dynamodb::table(self::LIKE_TABLE_NAME)->insert($data);
        var_dump($result);
    }

    /**
     * 查询 5089#1  下面的最新的20条点赞数据
     * User: ken xu
     * Date: 2022-10-09 11:05
     */
    public function query(){
        $fid = '5089#1';
        $result = Dynamodb::table(self::LIKE_TABLE_NAME,self::GSI_FID)->kwhere('fid=:v')->expressValue([':v'=>$fid])->limit(20,[])->order(false)->query();
        var_dump($result);
    }

    /**
     * 知道pk,sk 单独查询一个item
     * 注意：我的like表没有设置sk,所以只要一个pk就行
     * User: ken xu
     * Date: 2022-10-09 11:14
     */
    public function one(){
        $lid = '5089#1up9i3as7tbc45bbfr0u82td#1';
        $key = ['lid' => $lid];
        $result = Dynamodb::table(self::LIKE_TABLE_NAME)->kWhere($key)->one();
        var_dump($result);
    }

    /**
     * 知道pk,sk 查询多个item
     * 注意：我的like表没有设置sk,所以只要一个pk就行
     * User: ken xu
     * Date: 2022-10-09 11:14
     */
    public function ones(){
        $lid = '5089#1up9i3as7tbc45bbfr0u82td#1';
        $lid2 = '5089#demo1up9i3as7tbc45bbfr0u82td#1';
        $keys = [
            ['lid' => $lid],
            ['lid' => $lid2],
        ];
        $result = Dynamodb::table(self::LIKE_TABLE_NAME)->kWhere($keys)->ones();
        var_dump($result);
    }


    /**
     * 修改  lid: 5089#1up9i3as7tbc45bbfr0u82td#1
     * User: ken xu
     * Date: 2022-10-09 10:54\
     */
    public function update(){
        $new_name = 'new-demo'.random_int(1000,9999);//修改name字段
        $lid = '5089#1up9i3as7tbc45bbfr0u82td#1';
        //expressValue和expressName 使用是防止用的单词是dynamodb的关键词
        //修改多个字段时，update为二维数组 ，例如 ： [['name'=>'new-name'],['gender'=>1]]
        $result =  Dynamodb::table(self::LIKE_TABLE_NAME)->expressValue([':name'=>$new_name])->kWhere(['lid'=>$lid])->expressName(['#name'=>'name'])->update(['#name',':name']);
        var_dump($result);
    }

    /**
     * 删除一个item,通过pk,sk   example:  lid: 5089#1up9i3as7tbc45bbfr0u82td#1
     * 注意：我的like表没有设置sk,所以只要一个pk就行
     * User: ken xu
     * Date: 2022-10-09 10:57
     */
    public function delete(){
        $lid = '5089#1up9i3as7tbc45bbfr0u82td#1';
        $result =  Dynamodb::table(self::LIKE_TABLE_NAME)->kwhere(['lid'=>$lid])->delete();
        var_dump($result);
    }

    /**
     * 删除多个item,通过pk,sk
     * User: ken xu
     * Date: 2022-10-09 11:20
     */
    public function mulDelete(){
        $lid = '5089#1up9i3as7tbc45bbfr0u82td#1';
        $lid2 = '5089#demo1up9i3as7tbc45bbfr0u82td#1';
        $keys = [
            ['lid' => $lid],
            ['lid' => $lid2],
        ];
        $result = Dynamodb::table(self::LIKE_TABLE_NAME)->kWhere($keys)->deletes();
        var_dump($result);
    }

}


$obj = new demo();
$obj -> query();