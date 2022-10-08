<?php
/**
 * 这只是简单的操作demo，一些内容已被缩减
 * This is just a simple operation demo, and some content has been reduced
 */

namespace app\components;

use app\models\Service;
use Yii;

class DFeed
{
    public static $feed_name = 'feed';//feed表名
    public static $like_name = 'like';//like表名
    public static $comment_name = 'comment';//comment表名
    public static $log_name = 'dynamo';//日志名
    
    // DynamoDB Feed表中 gsi_user 全局索引
    const GSI_USER = 'gsi_user';
    // DynamoDB Comment表 gsi_feed 全局索引
    const GSI_FEED = 'gsi_feed';
    // DynamoDB like表中 gsi_fid 全局索引
    const GSI_FID = 'gsi_fid';

    const DYNAMO_BATCH_MAX = 25; //dynamodb 批量操作最大值

    public function __construct()
    {
    }


    /**
     * [Create 创建timeline]
     * @author xdk <[email address]>
     * @param [array] $data [创建timeline的数组]
     * @param [bool] $is_now_up [是否立即分发到用户好友的timeline]
     */
    public static function Create($data=[],$is_now_up = true)
    {
        //内容，发布人信息必须
        if(!isset($data['uid']) || empty($data['uid']) || !isset($data['cnt']) || empty($data['cnt']) || !isset($data['name']) || empty($data['name']) || !isset($data['avatar']) || empty($data['avatar'])){
            \Yii::info('Missing parameters in action Create', self::$log_name);
            return false;
        }
        //其他信息没传进行初始化
        $data['imgurls'] = isset($data['imgurls']) ?  $data['imgurls'] : null;
        $data['like'] =  isset($data['like']) ? intval($data['like']) : 0;
        $data['created'] =  time();
        $data['fid'] = isset($data['fid']) ? $data['fid'] ;
        $data = self::CheakData($data);
        $result = Dynamodb::table(self::$feed_name)->insert($data);
        if($result){

            return $data;
        }else{
            // \Yii::info('insert dynamodb fail in action Create', "feed_dynamo");
            return false;
        }
    }

    /**
     * [Update 更新timeline]
     * @author xdk <[email address]>
     * @param [array] $data [修改的timeline的数组]
     * @param [array] $oldfeed [dynamodb里面存的数据]
     */
    public static function Update($data=[],$oldfeed=[])
    {
        if(!self::check($data)){
          \Yii::info('$data check false is null in action update',  self::$log_name);
           return false;
        }
        $is_need_replaces = 0;
        //允许需改 内容，图片地址这些（'imgurls','cnt','uers','mentions'）
        $up_arr = [];
        $allow_up_params = ['cnt','created'];
        foreach ($data as $key => $value) {
            if(in_array($key,$allow_up_params)){
                $up_arr[$key] = $value;
                if( $value === null || $value === '' || $value === '[]'){
                    $is_need_replaces = 1;
                }
            }
        }
        if(!$up_arr){
            //没有修改的内容，直接返回false
            \Yii::info('fid=['.$data['fid'].'] No changes in action update',  self::$log_name);
            return false;
        }
        $result = false;
        //如果is_need_replaces为true，说明有原值变为空值的，需要覆盖原item
        if($is_need_replaces){
            //先找到原先的item
            if(empty($oldfeed)){
                \Yii::info('$oldfeed is null in action update',  self::$log_name);
                 return false;
            }
            $updata = [];
            foreach ($oldfeed as $key => $value) {
                 if(array_key_exists($key,$up_arr)){
                    if($up_arr[$key] === null || $up_arr[$key] === '' || $up_arr[$key] === '[]'){
                        continue;
                    }else{
                        $updata[$key] = $up_arr[$key];
                    }
                 }else{
                     $updata[$key] = $value;
                 }
            }
            //修改时间覆盖创建时间
            $updata['created'] = time();
            //覆盖原先的
            $result = Dynamodb::table(self::$feed_name)->insert($updata);
            
        }else{
            $expressValue = $updata = $expressName = [];
            $i = 0;
            $up_arr['created'] = time();//修改时间覆盖创建时间
            foreach ($up_arr as $key => $value) {
                $f = '#f'.$i;
                $v = ':v'.$i;
                $expressName[$f] = $key;
                $updata[] = [$f,$v];
                $expressValue[$v] = $value;
                $i++;
            }
            $result = Dynamodb::table(self::$feed_name)->expressValue($expressValue)->kwhere(['fid'=>$data['fid']])->expressName($expressName)->update($updata);
        }
        if($result){

            return true;
        }else{
            return false;
        }
    }
    
    /**
     * [Remove 删除timeline]
     * @author xdk <[email address]>
     * @param [array] $data [description]
     */
    public static function Remove($data=[])
    {
        if(!self::check($data)){
           return false;
        }
        $result = Dynamodb::table(self::$feed_name)->kwhere(['fid'=>$data['fid']])->delete();
        if($result){


            return true;
        }else{
            return false;
        }
    }
    
  
    /**
     * [addLookNum description修改浏览次数]
     * @param [type] $data [description]
     */
    public static function addLookNum($data=[]){
         if(!isset($data['fid'])){
            \Yii::info('params [fid] is null in action like',  self::$log_name);
            return false;
        }
        $result = Dynamodb::table(self::$feed_name)->expressValue([':v1'=>1])->kwhere(['fid'=>$data['fid']])->expressName(['#f1'=>'look'])->update(['#f1',':v1','+']);
        if($result){
            return true;
        }else{
            return false;
        }
    }
    /**
     * [Like timeline 点赞]
     * @author xdk <[email address]>
     * @param [array] $data [description]
     */
    public static function Like($data=[])
    {
        if(!isset($data['fid']) || empty($data['fid']) || !isset($data['uid']) || empty($data['uid']) || !isset($data['name']) || empty($data['name']) || !isset($data['avatar']) || empty($data['avatar'])){
            \Yii::info('params [fid or user info] is null in action like',  self::$log_name);
            return false;
        }
        $data['lid'] = $data['fid'].'#'.$data['uid'];//Service::create_guid();
        $data['created'] =  time();
        $result = Dynamodb::table(self::$like_name)->insert($data);
        if($result){
            //修改 dynamodb feed表like字段 + 1
            Dynamodb::table(self::$feed_name)->expressValue([':v1'=>1])->kwhere(['fid'=>$data['fid']])->expressName(['#f1'=>'like'])->update(['#f1',':v1','+']);
            return $data;
        }else{
            return false;
        }
    }

   /**
    * [Unlike 取消点赞]
    * @param [type] $data [description]
    */
   public static function Unlike($data=[]){
        if(!isset($data['fid']) || empty($data['fid']) || !isset($data['uid']) || empty($data['uid']) ){
            \Yii::info('params [fid or uid] is null in action unlike',  self::$log_name);
            return false;
        }
        $lid = $data['fid'].'#'.$data['uid'];
        $result = Dynamodb::table(self::$like_name)->kwhere(['lid'=>$lid])->delete();
        if($result){
            //修改 dynamodb feed表like字段 - 1
            Dynamodb::table(self::$feed_name)->expressValue([':v1'=>1])->kwhere(['fid'=>$data['fid']])->expressName(['#f1'=>'like'])->update(['#f1',':v1','-']);
            return true;
        }else{
            return false;
        }
   }
    
    
    /**
     * 判断用户user_id是否对某条feed feed_id点赞
     * @param $feed_id
     * @param $user_id
     * @return bool 点赞为true，不点赞为false
     */
    public static function isLike($feed_id, $user_id)
    {
        $item = $feed_id.'#'.$user_id;
        $key = ['lid' => $item];
        $flag =  Dynamodb::table(self::$like_name)->kwhere($key)->one();
        return !empty($flag)? true : false;
    }
    
    /**
     * 获取某条feed的点赞总数
     * @param $feed_id
     * @return int
     */
    public static function getLikeCount($feed_id)
    {
        if (empty($feed_id)) {
            Yii::info('empty feed id',  self::$log_name);
            return 0;
        }
        $key = ['fid' => $feed_id];
        $feed = Dynamodb::table(self::$feed_name)->kwhere($key)->one();
        return (!empty($feed['like']) && $feed['like']>0)? $feed['like'] : 0;
    }


    /**
     * [check 在修改删除前判断]
     * @author xdk <[email address]>
     * @param  [array] $data [description]
     * @return [type]       [description]
     */
    private static function check($data){
        if(empty($data)){
            return false;
        }
        //fid必须
        //$data['uid']为当前用户id
        if(!isset($data['fid']) || empty($data['fid']) || !isset($data['uid']) || empty($data['uid'])){
            \Yii::info('params [fid or uid] is null',  self::$log_name);
            return false;
        }
       
        return true;
    }
   

     /**
      * [getLikeByLid 通过lid来获取改条like信息]
      * @param  string $lid [description]
      * @return [type]      [description]
      */
    public static function getLikeByLid($lid=''){
        if($lid){
           return Dynamodb::table(self::$like_name)->kwhere(['lid'=>$lid])->one();
        }
        return null;
    }


    /**
     * [CheakData description]
     * 再更改dynamodb前检查data数据，去掉为空的
     * @param [type] $data [description]
     */
    public static function CheakData($data=[]){
        if($data){
            foreach ($data as $key => $value) {
                if($value === null || $value === '' || $value === '[]'){
                    unset($data[$key]);
                }
            }
        }
        return $data;
    }

    /**
     * [addFeedNumberParams 给feed表的int字段修改]
     * ['look'=>[1,'+'],'like'=>[1,'-']]
     * @param [array] $data [二维数组]
     * 
     */
    public static function addFeedNumberParams($fid='',$data=[]){
        if(empty($fid) || empty($data)){
            return false;
        }
        $allow_arr = ['cmnt','look','like','share','replay','dur'];
        $expressName = $expressValue = $update = [];
        $i = 0;
        foreach ($data as $key => $value) {
            $f = '#f'.$i;
            $v = ':v'.$i;
            if(in_array($key,$allow_arr)){
                $expressName[$f] = $key;
                $expressValue[$v] = $value[0];
                if(!isset($value[1])){
                    $value[1] = '+';//默认加
                }
                $update[$i] = [$f,$v,$value[1]];
            }
        }
        return Dynamodb::table(self::$feed_name)->expressValue($expressValue)->kwhere(['fid'=>$fid])->expressName($expressName)->update($update);
    }


    /**
     * 批量删除点赞
     * @param $feed_id
     * @return bool
     */
    public static function DelLikeAll($feed_id)
    {
        $data = Dynamodb::table(self::$like_name, self::GSI_FID)->select("lid")->kwhere(["fid" => $feed_id])->query();
        if (empty($data["data"])) {
            return true;
        }
        $delete_data = [];
        foreach ($data["data"] as $item) {
            $delete_data["lid"] = $item["lid"];
            if (count($delete_data) >= self::DYNAMO_BATCH_MAX) {
                $res = Dynamodb::table(self::$like_name)->deletes($delete_data);
                if (!$res) {
                    return false;
                }

                $delete_data = [];
            }
        }

        if (!empty($delete_data)) {
            $res = Dynamodb::table(self::$like_name)->deletes($delete_data);
            if (!$res) {
                return false;
            }
        }
        return true;
    }


/**
 * [getAllCommentByFeedid 通过feed_id查询改feed的所有评论信息]
 * @param  [type] $feed_id [description]
 * @param  [array] $last    [上次查询的keys，分页使用]
 * @param  [array] $result  [结果]
 * @param  [int] $maxnum  [获取个数]
 * @return [type]          [description]
 */
    public static function getAllCommentByFeedid($feed_id,$nickname='',$maxnum = 500,$last=[],&$result=[]){
        if($feed_id){
           if($nickname){
                $data = Dynamodb::table(self::$comment_name,self::GSI_FEED)->kwhere('fid=:v')
                        ->expressValue([':v'=>$feed_id,':v2'=>$nickname])->fwhere('contains(nick,:v2)')->limit(0,$last)->query();            
                }else{
                 $data = Dynamodb::table(self::$comment_name,self::GSI_FEED)->kwhere('fid=:v')
                    ->expressValue([':v'=>$feed_id])->limit(0,$last)->query();         
                }

            if($data['data']){
                if(count($result) < $maxnum){
                    $result = array_merge($data['data'],$result);
                    if(!empty($data['lastEvaluatedKey'])){
                        self::getAllCommentByFeedid($feed_id,$nickname,$maxnum,$data['lastEvaluatedKey'],$result);
                    }
                }
            }
            return $result;
        }
        return [];
    }







}