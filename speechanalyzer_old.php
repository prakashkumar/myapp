<?php
include_once "DatabaseConfig.php"; 
include_once "elasticsearch.php";
ini_set('max_execution_time', 0);
class SpeechAnalyzer
{
	public $db = '';
    public $mysqli = '';	
	public $transcriptBasePath='';	 
	public $agent_channel=0;
	public $SessionId='';
    public $defaultTimeZone = 'UTC';
    public $isSubstituteKeyword=0;
    public $substitionList=[];
    public $datetimeFormat = 'M d,Y h:i:s A T';
	function __construct() {
		$this->db =     Database::getInstance();
		$this->mysqli = $this->db->getConnection();
		
		if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
		 
			$this->transcriptBasePath='D:/xampp/htdocs/VociJSON';	
		} else {
			 
			$this->transcriptBasePath='/vtanalytics/vociJSON';	
		}
		session_start();
		$this->SessionId= session_id();
		 
	
	}
	/*
	* function for speech analysis for all customers
	*/ 
	function analysisnew($customer_id='')
	{
		$customers=$this->getCustomers($customer_id);
        $this->substitionList = $this->getCustomerSetting($customer_id, 'subs_dollar');
		if(!empty($customers))
		{
			foreach ($customers as $customer)
			{				
				$this->updateRunStatus($customer_id,1);
				//upload open itmes				
				$result['upload_status']=$this->blukupload($customer);
				sleep(5);
				//analysis Assign items
				$result['analysis_status']=$this->ruleanalyzer($customer);
			    $this->updateRunStatus($customer_id,0);
			}
		}
	}
	
	function reanalysis($customer_id='',$startDate=null,$endDate=null)
	{

		if(!empty($customer_id))
		{
			$customers=$this->getCustomers($customer_id);
            $this->substitionList = $this->getCustomerSetting($customer_id, 'subs_dollar');
			if(!empty($customers))
				{
					foreach ($customers as $customer)
					{				 
						 
						$this->updateRunStatus($customer_id,1);
						$result=$this->ruleanalyzer($customer,'AUDITED',$startDate,$endDate);
						$this->updateRunStatus($customer_id,0);
										
					}
				} else{
				echo 'Another process is running currently...  ';
				echo 'Try again after 30 minutes. ';
			}
		}
	}
	
	/* 
	*	function for blukupload the speech analytic data to ES
	*/
	
	function blukupload($customer=null)
	{		
		if(!empty($customer))
		{
				echo $this->EventDateTime() .' - Upload start';
				echo "<br>\n";			
				$limit=100;			
			$AllcdrQueues=$this->getCdrServiceQueue($customer["cust_dbname"], $customer["master_account_no"],$customer["direction"],$batch_status='BLOP',$call_status='WINEW',null,null,$customer['source_code']);
			
			if(!empty($AllcdrQueues))
			{
				echo $this->EventDateTime() .' - Fech Cdr Open Items';
				echo "<br>\n";
					$Es = new Elasticsearch();				
					$cdrQueuesChunk=array_chunk($AllcdrQueues,$limit,true);
					 
					if(!empty($cdrQueuesChunk))
					{
					 foreach ($cdrQueuesChunk as $cdrQueues)
					 {						 
					
						$assignedCDR=array();						 
						$cdrdocuments=array();
						$transcripts=array();
						foreach ($cdrQueues as $cdr)
						{
							$cdr_details = $this->getCdrDetailsByID($customer["cust_dbname"], $cdr["cdr_id"]);
							$cdr_details_json = json_encode($cdr_details[0], JSON_NUMERIC_CHECK); 
							
							$cdrindex['index']['_index']=$customer["master_account_no"];
							$cdrindex['index']['_type']='cdr';
							$cdrindex['index']['_id']=$cdr_details[0]['cdr_GUID'];	
							
							$cdrdocuments['body'][]=$cdrindex;
							$cdrdocuments['body'][]=$cdr_details_json;
							
							$transcriptindex['index']['_index']=$customer["master_account_no"];
							//$transcriptindex['index']['_type']='transcript_'.$customer['direction'];
							$transcriptindex['index']['_type']='transcript';
							$transcriptindex['index']['_id']=$cdr_details[0]['cdr_GUID'];	
							$transcriptindex['index']['_parent']=$cdr_details[0]['cdr_GUID'];	
							
							$cdr_transcript=$this->getTranscriptByID($customer['folder_name'], $cdr_details[0]['cdr_GUID'],$customer["agent_channel"],$cdr_details[0]['file_location'],$cdr_details[0]['file_key'],$customer["cust_dbname"],$customer["id"],$cdr['callback_audio_format']);
						
							if(!empty($cdr_transcript))
							{
								
								$cdr_transcript=json_decode($cdr_transcript, true); 
								$cdr_transcript=json_encode($cdr_transcript, JSON_NUMERIC_CHECK); 
								$transcripts[]=$transcriptindex;
								$transcripts[]=$cdr_transcript;
								$assignedCDR[]=$cdr['id'];
							 
							}
						} 
									
						if(!empty($cdrdocuments) && !empty($assignedCDR))
						{
							$result = $Es->bulkPostdocument($customer["master_account_no"],$cdrdocuments);
								echo $this->EventDateTime() .' - Uploaded Cdr';
								echo "<br>\n";
							if(!empty($result) && !empty($transcripts))
							{ 					 
								$batchtranscripts=array_chunk($transcripts,20,true);
								if(!empty($batchtranscripts))
								{
									 foreach ($batchtranscripts as $transcript)
									 {
										$param['body']=$transcript;									 
										$result = $Es->bulkPostdocument($customer["master_account_no"],$param);	 
										 echo $this->EventDateTime() .' - Uploaded transcript';
										 echo "<br>\n";
									 }
										
								} 
								if(!empty($result))
								{ 	
									$this->updateCdrServiceQueueStatus($customer["cust_dbname"], $assignedCDR,'BLIP','WIASD');
									echo $this->EventDateTime() .' - Updated Status as Assigned of '.sizeof($assignedCDR).' calls';
									echo "<br>\n";
								}
							
							}
						} 
					}
				}
			}			
		}
	} 
	/* 
	*	function for upload the speech analytic data to ES
	*/
	
	function upload($customer=null)
	{		
		if(!empty($customer))
		{
			$cdrQueues=$this->getCdrServiceQueue($customer["cust_dbname"], $customer["master_account_no"],$customer["direction"],$batch_status='BLOP',$call_status='',null,null,$customer['source_code']);
			 
			if(!empty($cdrQueues))
			{
				$Es = new Elasticsearch();
				foreach ($cdrQueues as $cdr)
				{
					$cdr_details = $this->getCdrDetailsByID($customer["cust_dbname"], $cdr["cdr_id"]);
					$cdr_details_json = json_encode($cdr_details[0], JSON_NUMERIC_CHECK); 
					
				 	$result = $Es->postdocument($customer["master_account_no"],'cdr',$cdr_details[0]['cdr_GUID'],$cdr_details_json);
					 
					if(!empty($result) && isset($result['_id']))
					{ 
						$cdr_transcript=$this->getTranscriptByID($customer['folder_name'], $cdr_details[0]['cdr_GUID'],$customer["agent_channel"],'','',$customer["cust_dbname"],$customer["id"]);
						
						//$result = $Es->postdocument($customer["master_account_no"],'transcript_'.$customer['direction'],$cdr_details[0]['cdr_GUID'],$cdr_transcript,$cdr_details[0]['cdr_GUID']);
						$result = $Es->postdocument($customer["master_account_no"],'transcript',$cdr_details[0]['cdr_GUID'],$cdr_transcript,$cdr_details[0]['cdr_GUID']);
						 
						if(!empty($result) && isset($result['_id']))
						{ 					
							$this->updateCdrServiceQueueStatus($customer["cust_dbname"], $cdr["id"],'BLIP','WIASD');
						}
						else
							continue;
					}
				}
			}			
		}
	}
	/*
	* 	function for analysis the speech analytic data 
	*/
	function ruleanalyzer($customer=null,$workitem_status='WIASD',$startDate=null,$endDate=null)
	{
		echo $this->EventDateTime() .' - Analysis start';
		echo "<br>\n";
		if(!empty($customer))
		{
			$limit=100;	
			$AllcdrQueues=$this->getCdrServiceQueue($customer["cust_dbname"], $customer["master_account_no"],$customer["direction"],$batch_status='BLIP',$workitem_status,$startDate,$endDate,$customer['source_code']);
		
		echo $this->EventDateTime() .' -  fetch CDR List';
		echo "<br>\n";
			// get Speech analytic libraries
			$SALibraries=$this->getLibraries($customer["cust_dbname"]);
			echo $this->EventDateTime() .' -  get Library List';
			echo "<br>\n";
			$SpeechAnalyticResult=array();
			$SpeechAnalyticKeywordResult=array();
			$agent_channel=0;
			$processedcalls=0;
			if(!empty($AllcdrQueues) && !empty($SALibraries))
				{ 		
					$cdrQueuesChunk=array_chunk($AllcdrQueues,$limit,true);
				 if(!empty($cdrQueuesChunk))
				 {
					foreach ($cdrQueuesChunk as $cdrQueues)
					{		
								 
								$cdr_GUIDs=array();
								$cdr_ids=array();
								$SpeechAnalyticResult=array();
								$SpeechAnalyticKeywordResult=array();
                                $cdrIdList = '';
                                $callDates = array();
								foreach ($cdrQueues as $cdr)
								{
                                    $cdrIdList .=$cdr['cdr_id'].',';
									$cdr_GUIDs[$cdr['cdr_GUID']]=$cdr['id'];
									$cdr_ids[$cdr['cdr_GUID']]=$cdr['cdr_id'];
									$callDates[$cdr['cdr_GUID']]=$cdr['calldate'];

								}
								$searchindex=$this->createReadonlyIndex($customer,$cdr_GUIDs);
								$customer["searchindex"]=$searchindex;
								foreach ($SALibraries as $library_id=>$SALibrary)
								{
									
									$libraryElements=$this->getLibraryElements($customer["cust_dbname"],$library_id);
									echo $this->EventDateTime() .' -  library Elements of'.$SALibrary;
									echo "<br>\n";
									if(!empty($libraryElements))
									{
										 
										foreach($libraryElements as $element_id=>$element_name)
										{

											$applicable_cdr_GUIDs=$this->getApplicableCalls($customer,$element_id,$cdr_GUIDs,$customer["agent_channel"]);
											$query=$this->buildESQuery($customer["cust_dbname"],$applicable_cdr_GUIDs,$element_id,$customer["agent_channel"]);
											echo $this->EventDateTime() .' -  Build query for Element '.$element_name;
											echo "<br>\n";
											if(!empty($query))
											{	 
												$Es = new Elasticsearch();
												//$result=$Es->searchdocument($customer["master_account_no"],'transcript_'.$customer["direction"],$query);																		
												$result=$Es->searchdocument($customer["searchindex"],'transcript',$query);
												echo $this->EventDateTime() .' - get Result for Element '.$element_name;
												echo "<br>\n";
												$result=json_decode($result,true);
												if(!empty($result) && isset($result['hits']['hits']) && !empty($result['hits']['hits']))
												{ 
													 
													foreach($result['hits']['hits'] as $cdr)
													{
														
														if(array_key_exists($cdr['_id'],$applicable_cdr_GUIDs))
														{
															
															$SpeechAnalyticResult[$applicable_cdr_GUIDs[$cdr['_id']]]['elements'][$element_id]['result']='Y';
															$SpeechAnalyticResult[$applicable_cdr_GUIDs[$cdr['_id']]]['elements'][$element_id]['score_type']=1;
															$SpeechAnalyticResult[$applicable_cdr_GUIDs[$cdr['_id']]]['cdr_id']=$cdr_ids[$cdr['_id']];
															$SpeechAnalyticResult[$applicable_cdr_GUIDs[$cdr['_id']]]['cdr_guid']=$cdr['_id'];
															
														}
														 
													}
													
												}	
													 
												foreach($applicable_cdr_GUIDs  as $GUID=>$service_queue_id)
													{
														 
														if(!isset($SpeechAnalyticResult[$service_queue_id]['elements'][$element_id]))
														{
															
															$SpeechAnalyticResult[$service_queue_id]['elements'][$element_id]['result']='N';
															$SpeechAnalyticResult[$service_queue_id]['elements'][$element_id]['score_type']=0;
															$SpeechAnalyticResult[$service_queue_id]['cdr_id']=$cdr_ids[$GUID];
															$SpeechAnalyticResult[$service_queue_id]['cdr_guid']=$GUID;															
															
														}
														 
													}
												 
													//Keyword analysis
												$SpeechAnalyticKeywordResult[]=$this->keywordanalyzer($customer,$applicable_cdr_GUIDs,$cdr_ids,$element_id,$customer["agent_channel"]);

												echo $this->EventDateTime() .' -  Start keyword analysis for'.$element_name;
												echo "<br>\n";		
											} 
										}
										
									}
									
								}
								
							if(!empty($SpeechAnalyticResult))
							{
								  
								  
								$this->updateSpeechAnalyticResult($customer["cust_dbname"], $SpeechAnalyticResult);		
								echo $this->EventDateTime() .' -  Updated result to DB of '.sizeof($SpeechAnalyticResult).' calls';
								echo "<br>\n"; 
								
								$this->updateCallResult($customer["cust_dbname"],$customer["master_account_no"],$cdr_GUIDs,$customer["direction"],$customer["searchindex"],$cdr_ids);
								echo $this->EventDateTime() .' -  Updated call analysis data to ES of '.sizeof($SpeechAnalyticResult).' calls';
								echo "<br>\n";
								
								$this->updateAuditResulttoES($customer["cust_dbname"],$customer["master_account_no"],$SpeechAnalyticResult);
								echo $this->EventDateTime() .' -  Updated result to ES of '.sizeof($SpeechAnalyticResult).' calls';
								echo "<br>\n";
								
								$this->updateFinalSpeechAnalyticResult($customer["cust_dbname"]);
								echo $this->EventDateTime() .' -  Updated final result to Mysql of '.sizeof($SpeechAnalyticResult).' calls';
								echo "<br>\n";
								
								$this->updateSpeechAnalyticKeywordResultToES($customer["cust_dbname"],$customer["master_account_no"], $SpeechAnalyticKeywordResult,$callDates);
								echo $this->EventDateTime() .' -  Updated Keyword result to ES of '.sizeof($SpeechAnalyticResult).' calls';
								echo "<br>\n";								
														
								$this->updateAnalysisStatus($customer["cust_dbname"],$batch_status='BLIP',$call_status='AUDITED');
								echo $this->EventDateTime() .' -  Updated audited status of '.sizeof($SpeechAnalyticResult).' calls';
								echo "<br>\n";
                                /*
                                 Decision analysis
                                 */
                                echo $this->EventDateTime() ." - Decision Analysis Uploading:";
                                if($this->_check_decision($customer["cust_dbname"])){
                                    $AppBaseDir=dirname(__FILE__);
                                    $cmd = "php " . $AppBaseDir . DIRECTORY_SEPARATOR ."..".DIRECTORY_SEPARATOR."..".DIRECTORY_SEPARATOR."yii decision-analyzer/decision 1 " . $customer['id']." '' '' '".rtrim($cdrIdList,',')."'";
                                    try{
                                        print_r(shell_exec($cmd));
                                    }catch (Exception $ex){
                                        echo $ex->getMessage();
                                    }

                                }else{
                                    echo $this->EventDateTime() ."Decision Analysis Template not found:";
                                }
							}	  
						$processedcalls=$processedcalls+sizeof($cdrQueues);
						echo $this->EventDateTime() .' -  Total processed Calls '.$processedcalls;
						echo "<br>\n";	
						
						$this->deleteReadonlyIndex($customer["searchindex"]);
					}			
				} 
			}
		}
	}
	function _check_decision($customer_schema)
    {
        $sqlQuery = "select id from ".$customer_schema.".tbl_ds_templates where status = 1 and call_type>0 and call_type is not null";
        $res_raw = mysqli_query($this->mysqli, $sqlQuery);
        if($res_raw)
        {
            if ($res_raw->num_rows > 0) {
                return true;
            }
        }
        return false;
    }
	
	/*
	* function for getting speech analytic libraries 
	*/
	function getLibraries($customer_schema)
	{
		$sql_query = "select id,name from ".$customer_schema.".tbl_sa_libraries where lvl>0 and status=1 order by root";
	 
		$res_raw = mysqli_query($this->mysqli, $sql_query);		
		if($res_raw)
		{
			$res = array();
			if ($res_raw->num_rows > 0) {			
				while($row = mysqli_fetch_assoc($res_raw)) {				
					$res[$row['id']]=$row['name'];				
				}
			}
			return $res;	
		}
	}
	/*
	* function for getting library elements	
	*/
	function getLibraryElements($customer_schema,$library_id)
	{
		if(!empty($customer_schema) && !empty($library_id))
		{
				
		$sql_query = "select id,sa_element_name from ".$customer_schema.".tbl_sa_library_elements where sa_library_id=$library_id and status=1 order by sa_element_order_no;";		 
		
		$res_raw = mysqli_query($this->mysqli, $sql_query);
			
		$res = array();
		if ($res_raw->num_rows > 0) {			
			while($row = mysqli_fetch_assoc($res_raw)) {				
				$res[$row['id']]=$row['sa_element_name'];				
			}
		}		  
		return $res;	
		
		}
	}
	
	
	/*
	* function for build Elasticsearch Query
	*/
	function buildESQuery($customer_schema,$cdr_GUIDs,$element_id,$agent_channel)
	{
			 
		if(!empty($customer_schema) && !empty($cdr_GUIDs) && !empty($element_id))
		{
			$query=$this->_getBaseESQuery($customer_schema,$cdr_GUIDs,$element_id,$agent_channel);

			$fields=$this->_getfields();			 
			if(!empty($query))
			{
				$query='{"query":'.$query.',"fields" : '.$fields.'}';		
				return $query;
			}
			
		}
	}
	/*
	* function for _getBaseESQuery 
	*/
	function _getBaseESQuery($customer_schema,$cdr_GUIDs,$element_id,$agent_channel)
	{
			 
		if(!empty($customer_schema) && !empty($cdr_GUIDs) && !empty($element_id))
		{
			$query=$this->_getquery($customer_schema,$cdr_GUIDs,$element_id,$agent_channel);
			$filter=$this->_getFilter($customer_schema,$cdr_GUIDs,$element_id,$agent_channel);
			if(!empty($query))
			{
					$query=
					'{
						"filtered":
						{
							"query":'.$query.(!empty($filter)? ','.$filter:'').'												
						}
					}';		
						 
				return $query;
			}
			
		}
	}
	/*
	* function for build Elasticsearch filter Query
	*/
	function buildESFilterQuery ($customer_schema,$cdr_GUIDs,$element_id,$agent_channel)
	{	 
		if(!empty($customer_schema) && !empty($cdr_GUIDs) && !empty($element_id))
		{	 			
			$filter=$this->_getFilter($customer_schema,$cdr_GUIDs,$element_id,$agent_channel);			 
			if(!empty($filter))
			{
					$query=
				'{
					"query":
					{
						"filtered":
						{
							"query":{"match_all":{}}'.(!empty($filter)? ','.$filter:'').'												
						}
					},
					"fields" : ["_id" ]
				}';						
				 
				return $query;
			}
			
		}
	
	}
	
	function _getfields()
	{
		return '[ "app_data.client_gender",
       "app_data.agent_gender",
       "app_data.agent_clarity",
       "app_data.client_clarity",
       "app_data.client_emotion",
        "app_data.agent_emotion",
        "app_data.overall_emotion",
        "app_data.overtalk",
        "app_data.words",
        "app_data.silence",
		"sentiment"
		]';		
	}
	function _getFilter($customer_schema,$cdr_GUIDs,$element_id,$agent_channel)
	{
		$filterIds=$this->_getIDFilter($cdr_GUIDs);		
		$fieldFilter=$this->_getfieldFilter($customer_schema,$element_id);
		$GroupFilter=$this->_getgroupFilter($customer_schema,$element_id);

		$specialConditionFilter=$this->_getSpecialconditionFilter($customer_schema,$cdr_GUIDs,$element_id,$agent_channel);


		$filter='"filter":{	
				"bool": {
					"must": [
						'.(!empty($filterIds)?$filterIds:'').(!empty($fieldFilter)?','.$fieldFilter:'').(!empty($GroupFilter)?','.$GroupFilter:'').(!empty($specialConditionFilter)?','.$specialConditionFilter:'').'
						]
						}		 
				}';			 
		return $filter;
	}
	function _getquery($customer_schema,$cdr_GUIDs,$element_id,$agent_channel)
	{
		$rule_positive	=$this->_getRule($customer_schema,$cdr_GUIDs,$element_id,$agent_channel,$field_name='rule_search_engine');
		$rule_negative	=$this->_getRule($customer_schema,$cdr_GUIDs,$element_id,$agent_channel,$field_name='rule_negative_search_engine');
		$rule_neutral	=$this->_getRule($customer_schema,$cdr_GUIDs,$element_id,$agent_channel,$field_name='rule_neutral_search_engine');
		$rule			=null;
		 		
		 if(!empty($rule_positive))
		 {
			 $rule[]=$rule_positive;
		 }
		  if(!empty($rule_negative))
		 {
			 $rule[]=$rule_negative;
		 }
		  if(!empty($rule_neutral))
		 {
			 $rule[]=$rule_neutral;
		 }
		 
		if(!empty($rule))
		{
			/* $query='{
			"nested": {
				"path": "utterances",
				"query": {
					"bool": {
						"must": ['.$rule.'
						
					   ]
                } 
               
            } 
			}
		 }';		 */
		
	 $rule_str=implode(',',$rule);
	 $query='{  
					"bool": {
					"should": ['.$rule_str.'
						]
					} 
		 }';		 

			return $query;
		}		
	}
	
	function SpaceClean($string) {
		
		  $string=  preg_replace('/[Ã‚Â ]/', ' ', $string);
		  //$string = str_replace(' ', '-', $string); 
          //$string= str_replace('-',' ',preg_replace('/[^A-Za-z0-9\-\_]/', '', $string)); 
          $string= trim(preg_replace('!\s+!', ' ', $string));
		  return $string; 			 
	}
	
	function termClean($string) {
		
		  $string=  preg_replace('/[Ã‚Â ]/', '-', $string);
		  $string = str_replace(' ', '-', $string); 
          $string= str_replace('-',' ',preg_replace('/[^A-Za-z0-9\-\_$\']/', '', $string));
          $string= trim(preg_replace('!\s+!', ' ', $string));
		  return strtolower($string);
	}
    function substitute($keyword){
        $this->isSubstituteKeyword=0;
        if(!empty($this->substitionList)) {
            foreach($this->substitionList as $subKey=>$subVal) {
                if(strpos($keyword, $subKey) !== false) {
                    $keyword = preg_replace("([{$subKey}]+)",$subVal , $keyword);
                    $this->isSubstituteKeyword=1;
                }
            }
        }
        return $keyword;
    }
	function Cleannonprintable($string) {
		
		  $string=preg_replace('/[[:^print:]]/', '', $string);
		  return $string; 			 
	}
	function _addnearby($keyword=null,$channel=null,$isPrefix=null)
	{
		$query=array();
	 if($channel==1 || $channel==0)
	 {

         $keyword = $this->substitute($keyword);
         $keyword = $this->termClean($keyword);

		if(!empty($keyword) && strpos($keyword, '_'))
		{
		    $temp1=explode(' ',$keyword);
			if(!empty($temp1))
			{ 
					foreach ($temp1 as $term)
					{  
						if(strpos($term, '_'))
						{
							$temp2=explode('_',$term);
							$query2=array();
							 foreach ($temp2 as $term2)
							{
								$query2['span_near']['clauses'][]['span_term']['utterances.events.word_'.$channel]=$term2;		
							}
							if(!empty($query2['span_near']['clauses']))
								{
									$query2['span_near']['slop']=12;
									$query2['span_near']['in_order']='true';
									$query2['span_near']['collect_payloads']='false';
									$query ['span_near']['clauses'][]=$query2;
								}
						}
						else
						$query['span_near']['clauses'][]['span_term']['utterances.events.word_'.$channel]=$term;
					}
					if(!empty($query['span_near']['clauses']))
					{
						$query['span_near']['slop']=0;
						$query['span_near']['in_order']='true';
						$query['span_near']['collect_payloads']='false';
					}
					else
					{
                        if($isPrefix==1 ) {
                            $query['match_phrase_prefix']['utterances.events.word_' . $channel] = $keyword;
                        } else {
                            $query['match_phrase']['utterances.events.word_' . $channel] = $keyword;
                        }
					}
			}
			else
					{

                        if($isPrefix==1 ) {
                            $query['match_phrase_prefix']['utterances.events.word_'.$channel]=$keyword;
                        }else {
                            $query['match_phrase']['utterances.events.word_'.$channel]=$keyword;
                        }

					}
			$finalquery['bool']['must'][]=$query;
			
			return $finalquery;
		}
		else{
            if($isPrefix==1) {
                $finalquery['match_phrase_prefix']['utterances.events.word_' . $channel] = $keyword;
            }else{
                $finalquery['match_phrase']['utterances.events.word_'.$channel]=$keyword;
            }

			return $finalquery;
		}	
	  }
	  else
	  {
          if($isPrefix==1) {
              $finalquery['bool']['should'][]['match_phrase_prefix']['utterances.events.word_0']=$keyword;
              $finalquery['bool']['should'][]['match_phrase_prefix']['utterances.events.word_1']=$keyword;
          } else {
              $finalquery['bool']['should'][]['match_phrase']['utterances.events.word_0'] = $keyword;
              $finalquery['bool']['should'][]['match_phrase']['utterances.events.word_1'] = $keyword;
          }
		  return $finalquery;
	  }
	}
	function _makeSubquery($customer_schema,$cdr_GUIDs,$query,$agent_channel=0,$field)
	{
		if(!empty($query))
		{
			//$query=json_decode($query,1);
			$newQuery=array();
			$newQuery=$query;

			foreach($query as $key1=>$q1)
			{
				foreach($q1 as $key2=>$q2)
				{
					foreach($q2 as $key3=> $q3)
					{
							 
								if(isset($q3['match_phrase']['wordagent']))
								{	
									$query[$key1][$key2][$key3]['bool']['should'][]=$this->_addnearby($q3['match_phrase']['wordagent'],$agent_channel);
									
									// $query[$key1][$key2][$key3]['bool']['must'][]['term']['utterances.metadata.channel']['value']=$agent_channel;
									unset($query[$key1][$key2][$key3]['match_phrase']);
								}
								else if(isset($q3['term']['app_data.agent_emotion']))
								{

									//print_r($q3['term']['app_data.agent_emotion']);
									$query[$key1][$key2][$key3]['bool']['should'][]['match']['app_data.agent_emotion']= $q3['term']['app_data.agent_emotion'];
									// $query[$key1][$key2][$key3]['bool']['must'][]['term']['utterances.metadata.channel']['value']=($agent_channel==0?1:0);
									unset($query[$key1][$key2][$key3]['term']);
								}
								else if(isset($q3['term']['app_data.client_emotion']))
								{

									//print_r($q3['term']['app_data.client_emotion']);
									$query[$key1][$key2][$key3]['bool']['should'][]['match']['app_data.client_emotion']= $q3['term']['app_data.client_emotion'];
									// $query[$key1][$key2][$key3]['bool']['must'][]['term']['utterances.metadata.channel']['value']=($agent_channel==0?1:0);
									unset($query[$key1][$key2][$key3]['term']);
								}
								else if(isset($q3['term']['app_data.overall_emotion']))
								{

									//print_r($q3['term']['app_data.overall_emotion']);
									$query[$key1][$key2][$key3]['bool']['should'][]['match']['app_data.overall_emotion']= $q3['term']['app_data.overall_emotion'];
									// $query[$key1][$key2][$key3]['bool']['must'][]['term']['utterances.metadata.channel']['value']=($agent_channel==0?1:0);
									unset($query[$key1][$key2][$key3]['term']);
								}
								else if(isset($q3['range']['app_data.client_clarity']))
								{

									//print_r($q3['term']['app_data.agent_emotion']);
									$query[$key1][$key2][$key3]['bool']['should'][]['range']['app_data.client_clarity']= $q3['range']['app_data.client_clarity'];
									// $query[$key1][$key2][$key3]['bool']['must'][]['term']['utterances.metadata.channel']['value']=($agent_channel==0?1:0);
									unset($query[$key1][$key2][$key3]['range']);
								}
								else if(isset($q3['range']['app_data.agent_clarity']))
								{

									//print_r($q3['term']['app_data.agent_emotion']);
									$query[$key1][$key2][$key3]['bool']['should'][]['range']['app_data.agent_clarity']= $q3['range']['app_data.agent_clarity'];
									// $query[$key1][$key2][$key3]['bool']['must'][]['term']['utterances.metadata.channel']['value']=($agent_channel==0?1:0);
									unset($query[$key1][$key2][$key3]['range']);
								}
								else if(isset($q3['range']['app_data.silence']))
								{

									//print_r($q3['term']['app_data.agent_emotion']);
									$query[$key1][$key2][$key3]['bool']['should'][]['range']['app_data.silence']= $q3['range']['app_data.silence'];
									// $query[$key1][$key2][$key3]['bool']['must'][]['term']['utterances.metadata.channel']['value']=($agent_channel==0?1:0);
									unset($query[$key1][$key2][$key3]['range']);
								}
								else if(isset($q3['range']['app_data.overtalk']))
								{

									//print_r($q3['term']['app_data.agent_emotion']);
									$query[$key1][$key2][$key3]['bool']['should'][]['range']['app_data.overtalk']= $q3['range']['app_data.overtalk'];
									// $query[$key1][$key2][$key3]['bool']['must'][]['term']['utterances.metadata.channel']['value']=($agent_channel==0?1:0);
									unset($query[$key1][$key2][$key3]['range']);
								}
								else if(isset($q3['term']['cdr.direction']))
								{
									$query[$key1][$key2][$key3]['bool']['should'][0]['has_parent']['parent_type']="cdr"	;
									$query[$key1][$key2][$key3]['bool']['should'][0]['has_parent']['query']['term']['direction']=$q3['term']['cdr.direction'];

									unset($query[$key1][$key2][$key3]['term']);
								}
								else if(isset($q3['range']['cdr.duration']))
								{
									$query[$key1][$key2][$key3]['bool']['should'][0]['has_parent']['parent_type']="cdr"	;
									$query[$key1][$key2][$key3]['bool']['should'][0]['has_parent']['query']['range']['duration']=$q3['range']['cdr.duration'];

									unset($query[$key1][$key2][$key3]['range']);
								}
								else if(isset($q3['match_phrase']['wordconsumer']))
								{
									
									$query[$key1][$key2][$key3]['bool']['should'][]=$this->_addnearby($q3['match_phrase']['wordconsumer'],$agent_channel==0?1:0);
									// $query[$key1][$key2][$key3]['bool']['must'][]['term']['utterances.metadata.channel']['value']=($agent_channel==0?1:0);
									unset($query[$key1][$key2][$key3]['match_phrase']);
								}
								else if(isset($q3['match_phrase']['wordall']))
								{									

									$query[$key1][$key2][$key3]['bool']['should'][]=$this->_addnearby($q3['match_phrase']['wordall'],$agent_channel);
									$query[$key1][$key2][$key3]['bool']['should'][]=$this->_addnearby($q3['match_phrase']['wordall'],$agent_channel==0?1:0);
									unset($query[$key1][$key2][$key3]['match_phrase']);
									
								}
                                else if(isset($q3['match_phrase_prefix']['wordagent']))
                                {
                                    $query[$key1][$key2][$key3]['bool']['should'][]=$this->_addnearby($q3['match_phrase_prefix']['wordagent'],$agent_channel,1);

                                    // $query[$key1][$key2][$key3]['bool']['must'][]['term']['utterances.metadata.channel']['value']=$agent_channel;
                                    unset($query[$key1][$key2][$key3]['match_phrase_prefix']);
                                }
                                else if(isset($q3['match_phrase_prefix']['wordconsumer']))
                                {

                                    $query[$key1][$key2][$key3]['bool']['should'][]=$this->_addnearby($q3['match_phrase_prefix']['wordconsumer'],$agent_channel==0?1:0,1);
                                    // $query[$key1][$key2][$key3]['bool']['must'][]['term']['utterances.metadata.channel']['value']=($agent_channel==0?1:0);
                                    unset($query[$key1][$key2][$key3]['match_phrase_prefix']);
                                }
                                else if(isset($q3['match_phrase_prefix']['wordall']))
                                {

                                    $query[$key1][$key2][$key3]['bool']['should'][]=$this->_addnearby($q3['match_phrase_prefix']['wordall'],$agent_channel,1);
                                    $query[$key1][$key2][$key3]['bool']['should'][]=$this->_addnearby($q3['match_phrase_prefix']['wordall'],$agent_channel==0?1:0,1);
                                    unset($query[$key1][$key2][$key3]['match_phrase_prefix']);

                                }
								else if(isset($q3['term']['keyword_library']))
								{
									$subquery=$this->_getBaseESQuery($customer_schema,$cdr_GUIDs,$q3['term']['keyword_library'],$agent_channel);
									//$subquery=$this->_getRule($customer_schema,$cdr_GUIDs,$q3['term']['keyword_library'],$agent_channel,$field);																		
									if(!empty($subquery))
									$query[$key1][$key2][$key3]['bool']['should'][]=json_decode($subquery,1);
									unset($query[$key1][$key2][$key3]['term']);
							
								}else if(isset($q3['term']['compliance_library']))
								{
									$subquery=$this->_getBaseESQuery($customer_schema,$cdr_GUIDs,$q3['term']['compliance_library'],$agent_channel);								
									//$subquery=$this->_getRule($customer_schema,$cdr_GUIDs,$q3['term']['compliance_library'],$agent_channel,$field);								
									
									if(!empty($subquery))
									$query[$key1][$key2][$key3]['bool']['should'][]=json_decode($subquery,1);
									unset($query[$key1][$key2][$key3]['term']);
							
								}
								 else if(isset($q3['term']['identify_call_type_library']))
								{
									$subquery=$this->_getBaseESQuery($customer_schema,$cdr_GUIDs,$q3['term']['identify_call_type_library'],$agent_channel);	
									//$subquery=$this->_getRule($customer_schema,$cdr_GUIDs,$q3['term']['identify_call_type_library'],$agent_channel,$field);								
									
									if(!empty($subquery))
									$query[$key1][$key2][$key3]['bool']['should'][]=json_decode($subquery,1);
									unset($query[$key1][$key2][$key3]['term']);
							
								}
								 else if(isset($q3['term']['keyword_watch']))
								{
									$subquery=$this->_getBaseESQuery($customer_schema,$cdr_GUIDs,$q3['term']['keyword_watch'],$agent_channel);	
									//$subquery=$this->_getRule($customer_schema,$cdr_GUIDs,$q3['term']['keyword_watch'],$agent_channel,$field);								
									
									if(!empty($subquery))
									$query[$key1][$key2][$key3]['bool']['should'][]=json_decode($subquery,1);
									unset($query[$key1][$key2][$key3]['term']);
							
								}
								else if(isset($q3['bool']))
								{	 
									$query[$key1][$key2][$key3]=$this->_makeSubquery($customer_schema,$cdr_GUIDs,$query[$key1][$key2][$key3],$agent_channel,$field);
									 
								}
					}
				}
			}
			return $query;
		}

	}	
	
	
	function _makeSubFilter($customer_schema,$query)
	{
		if(!empty($query))
		{
			//$query=json_decode($query,1);
			$newQuery=array();
			$newQuery=$query;
			foreach($query as $key1=>$q1)
			{
				foreach($q1 as $key2=>$q2)
				{
					foreach($q2 as $key3=> $q3)
					{
							 
								  if(isset($q3['term']))
								{
									 									
									if(isset($q3['term']['cdr.direction']))
									{ 
									
										$query[$key1][$key2][$key3]['has_parent']['parent_type']="cdr"	;
										$query[$key1][$key2][$key3]['has_parent']['query']['term']['direction']=$q3['term']['cdr.direction'];
										unset($query[$key1][$key2][$key3]['term']);
									}
									else if(isset($q3['term']['cdr.phone_no']))
									{

										$query[$key1][$key2][$key3]['has_parent']['parent_type']="cdr"	;
										$query[$key1][$key2][$key3]['has_parent']['query']['bool']['should'][]['term']['caller']=$q3['term']['cdr.phone_no'];
										$query[$key1][$key2][$key3]['has_parent']['query']['bool']['should'][]['term']['called']=$q3['term']['cdr.phone_no'];
										unset($query[$key1][$key2][$key3]['term']);
									}
									else if(isset($q3['range']['cdr.duration']))
									{ 
									
										$query[$key1][$key2][$key3]['has_parent']['parent_type']="cdr"	;
										$query[$key1][$key2][$key3]['has_parent']['query']['range']['duration']=$q3['range']['cdr.duration'];
										unset($query[$key1][$key2][$key3]['term']);
									}
									else  
										{
											$query[$key1][$key2][$key3]=$q3;	
										}
									 
								}
								else if(isset($q3['range']['cdr.duration']))
									{ 
									
										$query[$key1][$key2][$key3]['has_parent']['parent_type']="cdr"	;
										$query[$key1][$key2][$key3]['has_parent']['query']['range']['duration']=$q3['range']['cdr.duration'];
										unset($query[$key1][$key2][$key3]['range']);
									}
								else if(isset($q3['bool']))
								{	 
									$query[$key1][$key2][$key3]=$this->_makeSubFilter($customer_schema,$query[$key1][$key2][$key3]);
									 
								}
					}
				}
			}		
			return  $query;			
		}
		
	}	 
	 
	function _getRule($customer_schema,$cdr_GUIDs,$element_id,$agent_channel,$field='rule_search_engine')
	{		  
		if(!empty($customer_schema) && !empty($element_id))
		{ 	
			$sql_query = "select ".$field." from ".$customer_schema.".tbl_sa_library_element_rules where sa_library_element_id=$element_id";
			$res_raw = mysqli_query($this->mysqli, $sql_query);
			$res = array();
			if ($res_raw->num_rows > 0) {			
				while($row = mysqli_fetch_assoc($res_raw)) {
						
					$query=$this->checkEmptyQuery($row[$field]);
					if(!empty($query));
					{
						$query=json_decode($query,1);

						$query=$this->_makeSubquery($customer_schema,$cdr_GUIDs,$query,$agent_channel,$field);
						$res[]=(!empty($query)?json_encode($query):null);
					}				
				} 		
				 		
						
				return  $res[0];
			}
		}		 
	}
	function _getIDFilter($cdr_GUIDs)
	{
		if(!empty($cdr_GUIDs))
		{
		$cdrids=array_keys($cdr_GUIDs);		 
		$cdrids= (json_encode($cdrids));
			$filter='{
						"ids":{"values": '.$cdrids.'}						
					}';				
		
		 return $filter;
		}
		
	}
	function checkEmptyQuery($query=null)
	{
		if(!empty($query))
		{	 
				$query=$this->SpaceClean($query);
		 		$query_json=json_decode($query,1);			 				
				if(isset($query_json['bool']) && empty($query_json['bool']))
				{
					return null;
				}
		}
		return $query;
	}
	function _getfieldFilter($customer_schema,$element_id)
	{		
		
		if(!empty($customer_schema) && !empty($element_id))
			{ 	
				$sql_query = "select filter_search_engine from ".$customer_schema.".tbl_sa_library_element_rules where sa_library_element_id=$element_id";


				$res_raw = mysqli_query($this->mysqli, $sql_query);
				$res = array();
				if ($res_raw->num_rows > 0) {
					while($row = mysqli_fetch_assoc($res_raw)) {
						$query=$this->checkEmptyQuery($row['filter_search_engine']);
					
							if(!empty($query));
							{
								$query=json_decode($query,1);
								$query=$this->_makeSubFilter($customer_schema,$query);								 
								$res[]=(!empty($query)?json_encode($query):null);
							}		
					}
					 
					return $res[0];	
				}
				
			}		  
	}
	
	/*
	* This function _getSpecialconditionFilter to get special condition query 
	*
	*/
	function _getSpecialconditionFilter($customer_schema,$cdr_GUIDs,$element_id,$agent_channel)
	{
		$SpecialconditionFilter=null;
			if(!empty($customer_schema) && !empty($element_id))
			{ 
				$SpecialconditionFilter=$this->_getRule($customer_schema,$cdr_GUIDs,$element_id,$agent_channel,$field_name='rule_special_condition_search_engine');
				if(!empty($SpecialconditionFilter))
				$SpecialconditionFilter='
					{ 
						 "query":'.$SpecialconditionFilter.'
					}';
			}
		return $SpecialconditionFilter;		 		  
	}
	/*
	* This function _getGroupfilter to get the element groups
	*
	*/
	
	function _getGroupfilter($customer_schema,$element_id)
	{	
		if(!empty($customer_schema) && !empty($element_id))
			{ 	 
				$filter=null;				 
				$res=$this->_getElementGroups($customer_schema,$element_id);
				if(!empty($res))
				{
					$ids='"'.str_ireplace(',','","',$res).'"';
					$filter='
					{
					  "has_parent": {
						 "type": "cdr",
						 "query": {
							 "terms": {
								"group_id": ['.$ids.']
							 }
						 }
					  }
					}';
					return $filter;
				}
				else
					return null; 
			}
		else
		return null;
	}
	
	/*
	* function get element groups 
	*/
	function _getElementGroups($customer_schema,$element_id)
	{
		if(!empty($customer_schema) && !empty($element_id))
		{
				$sql_query = "select applicable_agent_group_ids from ".$customer_schema.". tbl_sa_library_elements where id=$element_id";			
				$res_raw = mysqli_query($this->mysqli, $sql_query);
				$filter=null;
				$res = array();
				if ($res_raw->num_rows > 0) {
					while($row = mysqli_fetch_assoc($res_raw)) {
						$res[]=$row['applicable_agent_group_ids'];						
					}
						if(!empty($res[0]) && isset($res[0]))
						return $res[0];
						else
							return null;
					}
			else
				return null;
		}
		else
			return null;
	}
	/*
	* function for get customers service queue calls
	*/
	function getCdrServiceQueue($customer_schema,$master_acc_no,$direction,$batch_status,$call_status,$startDate=null,$endDate=null,$dataSource=null)
	{
		$sql_query = "SELECT a.*,d.callback_audio_format,b.cdr_GUID,b.agent_id,b.location_id,b.source_code,b.agent_location,b.agent_name,b.group_id,b.agent_group,REPLACE(b.calldate,' ','T') AS calldate FROM ".$customer_schema.".tbl_cdr_service_queue a
					join ".$customer_schema.".tbl_cdr b on (a.cdr_id=b.id)
					join tbl_customer_service_accounts c on(c.id=a.customer_service_account_id)
					join tbl_service_providers d on (d.id=c.service_provider_id)
					WHERE a.service_id=2 
					AND a.work_item_status_id = (select id from tbl_work_item_statuses b where b.status_code='".$batch_status."')";
					if(!empty($call_status))
						$sql_query.= " 	AND a.call_status = (select id from tbl_work_item_statuses b where b.status_code='".$call_status."')";
					else
						$sql_query.= " AND a.call_status is null";
					
					if(!empty($startDate))
						$sql_query.= " 	AND b.calldate >=' ".$startDate."'";
					 		
					if(!empty($endDate))
						$sql_query.= " 	AND b.calldate <=' ".$endDate."'";
                if(!empty($dataSource))
                    $sql_query.= " 	AND b.source_code ='".$dataSource."'";

		$sql_query.= " AND b.direction=$direction order by b.calldate";

		$res1 = mysqli_query($this->mysqli, $sql_query);
			
		$cdr_queues = array();
		if ($res1->num_rows > 0) {			
			while($row = mysqli_fetch_assoc($res1)) {				
				$cdr_queues[]=$row;				
			}
		}
		return $cdr_queues;	
	}
	/*
	* function for get applicable group calls 
	*/
	function getApplicableCalls($customer,$element_id,$cdr_GUIDs,$agent_channel)	
	{ 
		if (!empty($element_id) && !empty($cdr_GUIDs))
		{		
			$query=$this->buildESFilterQuery($customer["cust_dbname"],$cdr_GUIDs,$element_id,$agent_channel);
			$cdr_queues = array();
			if(!empty($query))
			{	 
				$Es = new Elasticsearch();											
				$result=$Es->searchdocument($customer["searchindex"],'transcript',$query);																							 
				$result=json_decode($result,true);
				if(!empty($result) && isset($result['hits']['hits']) && !empty($result['hits']['hits']))
				{ 
					 
					foreach($result['hits']['hits'] as $cdr)
					{
						$cdr_queues[$cdr['_id']]=$cdr_GUIDs[$cdr['_id']]; 
					}
					
				}	 
				return $cdr_queues;
			}
		}
		else
		{
			return $cdr_GUIDs; 
		}
	}
	
	/*
	* function for getting customers account details
	*/
	function getCustomers($id=null,$donot_check_sa_run_status=null) {
		
	return $this->getCustomerFolderSettings($id,$donot_check_sa_run_status);
	
		$sql_query = "select * from tbl_customers where status=1 ";
		if(!empty($id))
			$sql_query.=" and id=$id";
		
		$result = $this->mysqli->query($sql_query);		
		$customers = array();
		if ($result->num_rows > 0) {			
			while($row = $result->fetch_assoc()) {				
				$customers[] = $row;
			}
		}  
		return $customers;		
	}
	
	function getCustomerFolderSettings($id=null,$donot_check_sa_run_status=null) {
		$sql_query = "
		select Customer.id, 
		Customer.cust_dbname,
		Customer.master_account_no, 
		FolderSetting.direction, 
		FolderSetting.folder_name,
		DataSource.source_code,
		FolderSetting.agent_channel 
		from tbl_folder_settings as FolderSetting
		join tbl_customers as Customer on (FolderSetting.customer_id=Customer.id)
		join tbl_customer_data_sources as DataSource on (FolderSetting.data_source_id=DataSource.id)
		where FolderSetting.status=1
		and FolderSetting.service_id=2
		and Customer.status=1 		";
		if(empty($donot_check_sa_run_status))
		{
			$sql_query.= " and Customer.speech_analyzer_scheduler_status=0";
		}
		
		if(!empty($id))
			$sql_query.=" and Customer.id=$id";
		
		$result = $this->mysqli->query($sql_query);		
		$customers = array();
		if ($result->num_rows > 0) {			
			while($row = $result->fetch_assoc()) {				
				$customers[] = $row;
			}
		}  
		return $customers;		
	}	
	/*
	* function updateRunStatus for update the analyzer status
	*/
	function updateRunStatus($customer_id,$status=0)
	{
		$sql_query = " update tbl_customers set speech_analyzer_scheduler_status=$status where id=$customer_id;";		 		
		$this->mysqli->query($sql_query);
	}
	/*
	 * function updateOriginalFilePath for update original file path
	 */
	function updateOriginalFilePath($customer_schema, $cdrGUID, $original_file_path = '')
	{
		$sql_query = "UPDATE
			" . $customer_schema . ".tbl_cdr a
			JOIN " . $customer_schema . ".tbl_cdr_files b ON(a.id=b.cdr_id)
			SET b.original_file_location = '$original_file_path'
			WHERE a.cdr_GUID='$cdrGUID'";
		$this->mysqli->query($sql_query);
	}
	/*
	* function updateRedactDetails for update redacted file location
	*/
	function updateRedactDetails($customer_schema,$cdrGUID,$redact_file_path)
	{
		$sql_query = "UPDATE
			" . $customer_schema . ".tbl_cdr a
			JOIN " . $customer_schema . ".tbl_cdr_files b ON(a.id=b.cdr_id)
			SET b.file_location = '$redact_file_path'
			WHERE a.cdr_GUID='$cdrGUID'";
		$this->mysqli->query($sql_query);
	}
	/*
	* function for get the cdr completed details by CDR ID
	*/
	
	function getCdrDetailsByID($customer_schema, $cdr_id) {
		$sql_query = "		
		SELECT a.id, a.customer_service_account_id, a.batch_id, b.batch_name, a.cdr_GUID, REPLACE(a.calldate,' ','T') AS calldate, a.caller,
		a.called, a.duration, a.utilized_credit, a.direction, a.agent_id, a.agent_name, a.supervisor_id, a.supervisor_name,
		a.group_id,a.location_id,a.agent_location,a.agent_group, a.file_size, a.cdr_mode, a.source_code,  a.productive_duration, a.holdon_duration, a.is_invalid, a.is_exception_call,
		a.is_exception_call, REPLACE(a.created_date,' ','T') AS created_date, a.created_by, REPLACE(a.modified_date,' ','T') AS modified_date, a.modified_by,
		c.file_location,c.file_key
		FROM ".$customer_schema.".tbl_cdr as a 
		join ".$customer_schema.".tbl_customer_batches as b on (a.batch_id=b.id) 
		join ".$customer_schema.".tbl_cdr_files as c on (a.id=c.cdr_id)
		 WHERE a.id=".$cdr_id;
		
		$result = $this->mysqli->query($sql_query);
		$cdr_details = array();
		if ($result->num_rows > 0) {
			// output data of each row
			while($row = $result->fetch_assoc()) {
				$cdr_details[] = $row;
			}
		} else {
			printf("Errormessage: %s\n", $this->mysqli->error);
		}
		//print_r($cdr_details);
		
		return $cdr_details;
	}
	
	/*
	* function for update the cdr service queue status 
	*/
	function updateCdrServiceQueueStatus($customer_schema, $queue_id,$batch_status,$call_status) {
		$modifiedDate=$this->UTCDateTime();
		$sql_query = "UPDATE ".$customer_schema.".tbl_cdr_service_queue a 
		SET a.work_item_status_id=(select id from tbl_work_item_statuses b where b.status_code='".$batch_status."'),
		a.call_status=(select id from tbl_work_item_statuses b where b.status_code='".$call_status."'),
		a.modified_date='$modifiedDate'";
		if(is_array ($queue_id))
		{
			$sql_query.=" WHERE id in(".implode(',',$queue_id).")";
		}
		else
		{
			$sql_query.=" WHERE id=".$queue_id;
		}
		
		$result = $this->mysqli->query($sql_query);
		if(! $result) {
			printf("Errormessage: %s\n", $this->mysqli->error);
		} 
	} 
	/*
	* function for bulk update the cdr service queue status 
	*/
 
	function updateAnalysisStatus($customer_schema,$batch_status,$call_status) {
		$modifiedDate=$this->UTCDateTime();
		$sql_query = "UPDATE ".$customer_schema.".tbl_cdr_service_queue a, 
		".$customer_schema.".tbl_sa_library_element_rule_results_temp as b
		SET a.work_item_status_id=(select id from tbl_work_item_statuses where status_code='".$batch_status."' and status=1 and is_deleted=0),
			a.call_status=(select id from tbl_work_item_statuses where status_code='".$call_status."' and status=1 and is_deleted=0),a.modified_date='$modifiedDate'		
		WHERE a.id=b.cdr_service_queue_id;";		
		$result = $this->mysqli->query($sql_query);
		if(! $result) {
			printf("Errormessage: %s\n", $this->mysqli->error);
		} 
	} 
	/*
	* function for update the Speech Analytic Result status
	*/
	function updateSpeechAnalyticResult($customer_schema, $SpeechAnalyticResult)			
	{
		/*--- Creating Temporary Table for analysis result ---*/	
		$droptable="DROP TABLE IF EXISTS ".$customer_schema.".tbl_sa_library_element_rule_results_temp;";
		$res = $this->mysqli->query($droptable);
		if(! $res) {
			printf("Errormessage: %s\n", $this->mysqli->error);
			 
		}  
		$creattemptable="CREATE TABLE ".$customer_schema.".tbl_sa_library_element_rule_results_temp (					
						`id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Primary Key',
						`sa_library_element_id` INT(11) UNSIGNED NOT NULL,
						`cdr_id` INT(11) UNSIGNED NOT NULL ,
						`cdr_service_queue_id` INT(11) UNSIGNED NOT NULL ,	
						`audit_result` ENUM('Y','N') NULL DEFAULT NULL,
						`score_type` tinyint(3) unsigned NULL DEFAULT NULL,
						`cdr_GUID` VARCHAR(64) NOT NULL COMMENT 'cdr Global Unique ID'	,
						 PRIMARY KEY (`id`),
						INDEX `tbl_sa_library_element_rule_results_temp_sa_library_element_id` (`sa_library_element_id`),
						INDEX `tbl_sa_library_element_rule_results_temp_cdr_id` (`cdr_id`),
						INDEX `tbl_sa_library_element_rule_results_temp_cdr_service_queue_id` (`cdr_service_queue_id`),							
						INDEX `tbl_sa_library_element_rule_results_temp_cdr_GUID` (`cdr_GUID`)							
					)ENGINE=MyISAM DEFAULT CHARSET=utf8;";	 
		$res = $this->mysqli->query($creattemptable);
		if(! $res) {
			printf("Errormessage: %s\n", $this->mysqli->error);
			 
		} 				
		/*--- prepare result insert query ---*/
		if(!empty($SpeechAnalyticResult))
			{

				$insertquery="Insert into ".$customer_schema.".tbl_sa_library_element_rule_results_temp (sa_library_element_id,cdr_id,cdr_service_queue_id,audit_result,score_type,cdr_GUID) values ";
				$insertvaluequery='';		 
				foreach($SpeechAnalyticResult as $service_queue_id => $cdr)
				{
					foreach ($cdr['elements'] as   $element_id=>$result)
					{
						$insertvaluequery.="($element_id,".$cdr["cdr_id"].",$service_queue_id,'".$result['result']."','".$result['score_type']."','".$cdr["cdr_guid"]."'),";					
					}						 
				}

				if(!empty($insertvaluequery))
				{				 
					$insertquery.=substr($insertvaluequery,0,strlen($insertvaluequery)-1);
					
					$res = $this->mysqli->query($insertquery);
					if(! $res) {
						printf("Errormessage: %s\n", $this->mysqli->error);					 
					}
						else{
							
							/* Exclude the Not applicable elements from score card 	*/							
							// $this->excludeNotApplicableElements($customer_schema);	

							 
							/*--- Creating Temporary Table for analysis result ---*/	
							$droptable="DROP TABLE IF EXISTS ".$customer_schema.".temp_".$this->SessionId.";";
							$res = $this->mysqli->query($droptable);
							if(! $res) {
								printf("Errormessage: %s\n", $this->mysqli->error);
								 
							} 
								 
							$sql_query = "CREATE   TABLE ".$customer_schema.".`temp_".$this->SessionId."` (
										`id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Primary Key',
										`sa_library_element_id` INT(11) UNSIGNED NOT NULL COMMENT 'Foreign key to  sa_library_elements table',
										`cdr_id` INT(11) UNSIGNED NOT NULL COMMENT 'Foreign key to cdr table',
										`cdr_service_queue_id` INT(11) UNSIGNED NOT NULL COMMENT 'Foreign key to cdr service queue table',
										`min_score` DOUBLE(5,2) UNSIGNED NULL DEFAULT NULL COMMENT 'Minimum score',
										`max_score` DOUBLE(5,2) UNSIGNED NULL DEFAULT NULL COMMENT 'Maximum score',
										`sa_element_order_no` INT(10) UNSIGNED NULL DEFAULT NULL COMMENT 'Audit element order number',
										`is_consider_for_score_card` TINYINT(1) UNSIGNED NULL DEFAULT '0' COMMENT 'Is consider for score card if 1 is yes 0 is no',
										`audit_score` DOUBLE(5,2) UNSIGNED NULL DEFAULT NULL COMMENT 'Audit score',
										`audit_percentage` DOUBLE(5,2) UNSIGNED NULL DEFAULT NULL COMMENT 'Audit percentage',
										`audit_type` ENUM('M','A') NULL DEFAULT NULL COMMENT 'M- Manual , A- Auto',
										`audit_result` ENUM('Y','N') NULL DEFAULT NULL COMMENT 'Y- Pass , N -Fail',
										`work_item_status_id` INT(10) UNSIGNED NULL DEFAULT NULL COMMENT 'Work item status (work item)',
										`created_date` DATETIME NOT NULL COMMENT 'date of created',
										`created_by` INT(10) UNSIGNED NOT NULL COMMENT 'who created  ',
										`modified_date` DATETIME NOT NULL COMMENT 'date of modified',
										`modified_by` INT(10) UNSIGNED NOT NULL COMMENT 'who modified  ',
										PRIMARY KEY (`id`),
										INDEX `temp_".$this->SessionId."_sa_library_element_id` (`sa_library_element_id`),
										INDEX `temp_".$this->SessionId."_cdr_id` (`cdr_id`),
										INDEX `temp_".$this->SessionId."_cdr_service_queue_id` (`cdr_service_queue_id`)	
									)									 
									ENGINE=MyISAM DEFAULT CHARSET=utf8; ";

								$res1 = $this->mysqli->query($sql_query);
								if(! $res1) {
									printf("Errormessage: %s\n", $this->mysqli->error);								 
								}
								$now = $this->UTCDateTime();
								$sql_query = "insert into  ".$customer_schema.".temp_".$this->SessionId." 
											(sa_library_element_id,cdr_id,cdr_service_queue_id,min_score,max_score,sa_element_order_no,is_consider_for_score_card,audit_score,audit_percentage,audit_type,audit_result,work_item_status_id,created_date,created_by,modified_date,modified_by)
											( 
											select 
											AuditResult.sa_library_element_id,
											cdr_id,
											cdr_service_queue_id,
											(case when AuditScore.audit_score_id is null then 0 else AuditScore.minscore end) as minscore,
											(case when AuditScore.audit_score_id is null then 1 else AuditScore.maxscore end) as maxscore,									
											LibraryElement.lft as sa_element_order_no,
											(case when LibraryElement.is_consider_for_score_card is null then 0 else LibraryElement.is_consider_for_score_card end ) as is_consider_for_score_card,
											(case when AuditScore.audit_score_id is null then (case when AuditResult.score_type=1 then 1 else 0 end )else AuditScore.audit_score end ) as audit_score,
											(case when AuditScore.audit_score_id is null then (case when AuditResult.score_type=1 then 100 else 0 end )else AuditScore.audit_percentage end ) as audit_percentage,						 
											'A' as audit_type,
											AuditResult.audit_result,
											(select id from tbl_work_item_statuses b where b.status_code='BLCL') as work_item_status_id,
											'$now' as created_date,
											1 as created_by,
											'$now' as modified_date,
											1 as modified_by
											from ".$customer_schema.".tbl_sa_library_element_rule_results_temp as AuditResult
											join ".$customer_schema.".tbl_sa_library_elements as LibraryElement on (AuditResult.sa_library_element_id=LibraryElement.id)
											left join 
											(
												select  ObtainScore.sa_library_element_id,
														ObtainScore.score_type,
														ObtainScore.audit_score_id,
														ObtainScore.score_value as audit_score,
														round(ObtainScore.score_value/Elementscore.maxscore*100,2) as audit_percentage,
														Elementscore.maxscore,
														Elementscore.minscore
												from  ".$customer_schema.".tbl_sa_library_element_score as ObtainScore
												join (select sa_library_element_id,max(score_value) as maxscore, min(score_value) as minscore from  ".$customer_schema.".tbl_sa_library_element_score as a group by sa_library_element_id
													) as Elementscore on (ObtainScore.sa_library_element_id=Elementscore.sa_library_element_id)									
											) as AuditScore on (AuditResult.sa_library_element_id=AuditScore.sa_library_element_id and AuditResult.score_type=AuditScore.score_type)
											 );";
										 
									$res = $this->mysqli->query($sql_query);
									if(! $res) {
										printf("Errormessage: %s\n", $this->mysqli->error);
										 
									}
											
								
						}				
				}
			}	  
		 		 
	}
	
	function updateFinalSpeechAnalyticResult($customer_schema)
	{ 
			/*$sql_query = "
					delete from ".$customer_schema.".tbl_sa_library_element_rule_results
					where id in 
						(
							select id from (
							select AuditResult.id   
							from ".$customer_schema.".tbl_sa_library_element_rule_results_temp as tempResult
							join ".$customer_schema.".tbl_sa_library_element_rule_results as AuditResult on (tempResult.cdr_service_queue_id=AuditResult.cdr_service_queue_id)
							)  as a 
						);";*/
						
				$sql_query=	"Delete from ".$customer_schema.".tbl_sa_library_element_rule_results 
								where cdr_service_queue_id in (select distinct cdr_service_queue_id from ".$customer_schema.".tbl_sa_library_element_rule_results_temp) ;";
										
			
						 
				 $res = $this->mysqli->query($sql_query);
										if(! $res) {
											printf("Errormessage: %s\n", $this->mysqli->error);
											 
										} 	
										
				$sql_query = "insert into  ".$customer_schema.".tbl_sa_library_element_rule_results 
								(sa_library_element_id,cdr_id,cdr_service_queue_id,min_score,max_score,sa_element_order_no,is_consider_for_score_card,audit_score,audit_percentage,audit_type,audit_result,work_item_status_id,created_date,created_by,modified_date,modified_by)
								(select sa_library_element_id,cdr_id,cdr_service_queue_id,min_score,max_score,sa_element_order_no,is_consider_for_score_card,audit_score,audit_percentage,audit_type,audit_result,work_item_status_id,created_date,created_by,modified_date,modified_by
										from ".$customer_schema.".temp_".$this->SessionId."
									) ;";
							 
						$res = $this->mysqli->query($sql_query);
						if(! $res) {
							printf("Errormessage: %s\n", $this->mysqli->error);
							 
						}
						
				$droptable="DROP TABLE IF EXISTS ".$customer_schema.".temp_".$this->SessionId.";";
				$res = $this->mysqli->query($droptable);
				if(! $res) {
					printf("Errormessage: %s\n", $this->mysqli->error);
					 
				} 
		 		 
	}
	 
	function updateAuditResulttoES($customer_schema,$master_account_no,$SpeechAnalyticResult)
	{
		$auditresult=null;
		if(!empty($customer_schema) && !empty($SpeechAnalyticResult))
		{	
			$sql_query = " 
			select CallScore.cdr_GUID,
			CallScore.call_score  as audit_score_percentage,
			CallScore.acoustics_call_score,
			CallScore.is_payment_call,
			CallScore.calltype,
			CallScore.compliance_score,
			CallScore.compliance_audit_result
			from
				".$customer_schema.".tbl_sa_library_element_rule_results_temp as AuditTemp
				join ".$customer_schema.".tbl_sa_call_results as CallScore on (CallScore.cdr_GUID=AuditTemp.cdr_GUID)
				left outer join ".$customer_schema.".tbl_audit_performance_settings as Performance on  (CallScore.overall_emotion=Performance.performance and Performance.performance_analysis_type=4 and  Performance.status=1 and Performance.is_deleted=0)
				;";				  
			 $res = $this->mysqli->query($sql_query);
	 			if ($res->num_rows > 0) {
					// output data of each row
					 
					while($r = $res->fetch_assoc()) {					
												 			 
						$auditresult[$r['cdr_GUID']]['audit_percentage'] =$r['audit_score_percentage'];
						$auditresult[$r['cdr_GUID']]['acoustics_call_score'] =$r['acoustics_call_score'];
						$auditresult[$r['cdr_GUID']]['compliance_score'] =$r['compliance_score'];
						$auditresult[$r['cdr_GUID']]['compliance_audit_result'] =$r['compliance_audit_result'];
						$auditresult[$r['cdr_GUID']]['is_payment_call'] =$r['is_payment_call'];
						$auditresult[$r['cdr_GUID']]['calltype'] =$r['calltype'];
					}
				} else {
					printf("Errormessage: %s\n", $this->mysqli->error);
				} 
	
			$sql_query = " 
				select  
					AuditTemp.cdr_GUID,
					library.root as sa_library_id,
					parentlibrary.name as sa_library_name,
					sum(AuditResult.audit_score) as audit_score,
					sum(AuditResult.max_score) as max_score,
					round(sum(AuditResult.audit_score)/sum(AuditResult.max_score)*100,2)  as  audit_score_percentage,
					(case when sum(case when AuditResult.audit_result='N' then 1 else 0 end )>0 then 'N' else 'Y' end ) as audit_result 
					from 
				".$customer_schema.".temp_".$this->SessionId." as AuditResult
				join ".$customer_schema.".tbl_sa_library_element_rule_results_temp as AuditTemp on (AuditResult.cdr_service_queue_id=AuditTemp.cdr_service_queue_id and AuditResult.sa_library_element_id=AuditTemp.sa_library_element_id)
				join ".$customer_schema.".tbl_sa_library_elements as Element on (AuditResult.sa_library_element_id=Element.id)
				join ".$customer_schema.".tbl_sa_libraries as library on (Element.sa_library_id=library.id)
				join ".$customer_schema.".tbl_sa_libraries as parentlibrary on (parentlibrary.id=library.root)				
				 
				 group by 
				 AuditTemp.cdr_GUID,
				 library.root,
				parentlibrary.name 
				order by AuditTemp.cdr_GUID,library.root;
				";
				 
			 $res = $this->mysqli->query($sql_query);
	 			if ($res->num_rows > 0) {
					// output data of each row
					$i=0;
					while($r = $res->fetch_assoc()) {
						 				
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['sa_library_id'] =$r['sa_library_id'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['name'] =$r['sa_library_name'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['audit_score']=$r['audit_score'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['max_score']=$r['max_score'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['audit_score_percentage']=$r['audit_score_percentage'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['audit_result']=$r['audit_result'];
						//$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements']=$this->getElementResult($customer_schema ,$r['cdr_id'],$r['cdr_service_queue_id'],$r['sa_library_id']);						
						$i++;			
					}
				} else {
					printf("Errormessage: %s\n", $this->mysqli->error);
				}
				
			 $auditresult_elements=$this->getElementResult($customer_schema);
			 
			 //$auditComplianceScores=$this->getComplianceScore($customer_schema);
			 //$auditPaymentCalls=$this->isPaymentCall($customer_schema);
			 //$auditcalltypes=$this->getCallType($customer_schema);
			 
			 $auditresultFinal=array();
			 $auditresultdocs=array();
					
				if(!empty($auditresult))
				{
					foreach ($auditresult as $GUID =>$libraies )
					{
						 $auditindex['index']['_index']=$master_account_no;
						 $auditindex['index']['_type']='service_2';
						 $auditindex['index']['_id']= $GUID;	
						 $auditindex['index']['_parent']= $GUID;	
						 $auditresultdocs['body'][]=$auditindex;
						 $Libraryelements=array();		
						if(!empty($libraies['library']))
						{
							foreach ($libraies['library'] as $library_id=>$data)
							{	
														
								if(isset($auditresult_elements[$GUID]['library'][$library_id]['elements']))
								{
									$data['elements']=$auditresult_elements[$GUID]['library'][$library_id]['elements'];
									$Libraryelements[$GUID]['library'][]=$data;		
								}
								 
							}
						}
						
						 $libraies['library']=$Libraryelements[$GUID]['library'];
						// $libraies['compliance_score']=(isset($auditComplianceScores[$GUID])?$auditComplianceScores[$GUID]:null);
						// $libraies['is_payment_call']=(isset($auditPaymentCalls[$GUID])?$auditPaymentCalls[$GUID]:'N');						 
						// $libraies['calltype']=(isset($auditcalltypes[$GUID])?$auditcalltypes[$GUID]:'Other');						 
						 $auditresultdocs['body'][]=json_encode($libraies,JSON_NUMERIC_CHECK);
					}
				}
				 
			if(!empty($auditresultdocs))
			{
					//$auditresultdocs=json_encode($auditresultdocs,JSON_NUMERIC_CHECK);
					 
					$Es = new Elasticsearch(); 
					$result = $Es->bulkPostdocument($master_account_no,$auditresultdocs);
					
					//$result = $Es->postdocument($master_account_no,'service_2',$cdr_GUID,$auditresult,$cdr_GUID);					 
					
			}
		}
		
	}	
	 
	function getComplianceScore($customer_schema)
	{
		$auditresult=array();
		if(!empty($customer_schema))
		{
			
			$sql_query = " 
				select  	
					AuditTemp.cdr_GUID,
					round(sum(AuditResult.audit_score)/sum(AuditResult.max_score)*100,2)  as  audit_score_percentage					
					from 
				".$customer_schema.".tbl_sa_library_element_rule_results as AuditResult
				join ".$customer_schema.".tbl_sa_library_element_rule_results_temp as AuditTemp on (AuditResult.cdr_service_queue_id=AuditTemp.cdr_service_queue_id and AuditResult.sa_library_element_id=AuditTemp.sa_library_element_id)
				join ".$customer_schema.".tbl_sa_library_elements as Element on (AuditResult.sa_library_element_id=Element.id)
				join ".$customer_schema.".tbl_sa_libraries as library on (Element.sa_library_id=library.id)
				join ".$customer_schema.".tbl_sa_libraries as parentlibrary on (parentlibrary.id=library.root)
				 where library.root=(select var_value from ".$customer_schema.".tbl_sa_general_settings where var_name='compliance_library' and status=1)
				and AuditResult.is_consider_for_score_card=1
				 group by AuditTemp.cdr_GUID;
				";				
			 $res = $this->mysqli->query($sql_query);
	 			if ($res->num_rows > 0) {
					// output data of each row
					
					while($r = $res->fetch_assoc()) {
						 	
						$auditresult[$r['cdr_GUID']]=$r['audit_score_percentage'];			
					 		
					}
				} 
		}		 
		
			return $auditresult;
			  
	}
	function isPaymentCall($customer_schema)
	{	
		$auditresult=array();
		if(!empty($customer_schema))
		{	
			$sql_query = " 
				select 		
				AuditTemp.cdr_GUID,				
				AuditResult.audit_result				 
				from 
				".$customer_schema.".tbl_sa_library_element_rule_results as AuditResult
				join ".$customer_schema.".tbl_sa_library_element_rule_results_temp as AuditTemp on (AuditResult.cdr_service_queue_id=AuditTemp.cdr_service_queue_id and AuditResult.sa_library_element_id=AuditTemp.sa_library_element_id)
				join ".$customer_schema.".tbl_sa_library_elements as Element on (AuditResult.sa_library_element_id=Element.id)
				join ".$customer_schema.".tbl_sa_libraries as library on (Element.sa_library_id=library.id)
				join ".$customer_schema.".tbl_sa_libraries as parentlibrary on (parentlibrary.id=library.root)
				 where 
				   Element.id=(select var_value from ".$customer_schema.".tbl_sa_general_settings where var_name='is_payment_call' and status=1);				 
			";
			 $res = $this->mysqli->query($sql_query);
	 			if ($res->num_rows > 0) {
					// output data of each row					
					while($r = $res->fetch_assoc()) {							
					$auditresult[$r['cdr_GUID']]=$r['audit_result'];							
										
					}
				}
				 
		}
		
		return $auditresult;
	}
	function getCallType($customer_schema)
	{		
			$auditresult=array();
		if(!empty($customer_schema))
		{	
			$sql_query = "  
					select cdr_GUID,
					sa_element_name as calltype
					from (
						SELECT @row_number:=CASE WHEN @cdr_GUID=cdr_GUID THEN @row_number+1 ELSE 1 END AS row_number,@cdr_GUID:=cdr_GUID AS cdr_GUID,id,sa_element_name
						from 
									(select 		
										AuditTemp.cdr_GUID,				
										 Element.id ,
										 Element.sa_element_name			 			 
										from 
										".$customer_schema.".tbl_sa_library_element_rule_results as AuditResult
										join ".$customer_schema.".tbl_sa_library_element_rule_results_temp as AuditTemp on (AuditResult.cdr_service_queue_id=AuditTemp.cdr_service_queue_id and AuditResult.sa_library_element_id=AuditTemp.sa_library_element_id)
										join ".$customer_schema.".tbl_sa_library_elements as Element on (AuditResult.sa_library_element_id=Element.id)
										join ".$customer_schema.".tbl_sa_libraries as library on (Element.sa_library_id=library.id)
										join ".$customer_schema.".tbl_sa_libraries as parentlibrary on (parentlibrary.id=library.root)
										 where 					
											library.root=(select var_value from ".$customer_schema.".tbl_sa_general_settings where var_name='calltype_library' and status=1)				 
											and AuditResult.audit_result='Y'
										) as  callType , (SELECT @row_number:=0,@cdr_GUID:='') AS t
										order by cdr_GUID,id
						) as callType
					where row_number=1 ;";
				 
				 /*
				select 		
				AuditTemp.cdr_GUID,				
				Element.sa_element_name  as calltype				 
				from 
				".$customer_schema.".tbl_sa_library_element_rule_results as AuditResult
				join ".$customer_schema.".tbl_sa_library_element_rule_results_temp as AuditTemp on (AuditResult.cdr_service_queue_id=AuditTemp.cdr_service_queue_id and AuditResult.sa_library_element_id=AuditTemp.sa_library_element_id)
				join ".$customer_schema.".tbl_sa_library_elements as Element on (AuditResult.sa_library_element_id=Element.id)
				join ".$customer_schema.".tbl_sa_libraries as library on (Element.sa_library_id=library.id)
				join ".$customer_schema.".tbl_sa_libraries as parentlibrary on (parentlibrary.id=library.root)
				 where 					
				 library.root=(select var_value from ".$customer_schema.".tbl_sa_general_settings where var_name='calltype_library' and status=1)				 
				 and AuditResult.audit_result='Y'				 
				 order by Element.id asc limit 1
				 ;
				 
			"; */
			
			 $res = $this->mysqli->query($sql_query);
			  
	 			if ($res->num_rows > 0) {
					// output data of each row					
					while($r = $res->fetch_assoc()) {	
						$auditresult[$r['cdr_GUID']]=$r['calltype'];					 				
					}
				}				 
		}		 
		return $auditresult;
	}
	 function getElementResult($customer_schema)
	 {
		 $auditresult=null;
		if(!empty($customer_schema))
		{	
			$sql_query = " 
			select 
				AuditTemp.cdr_GUID,
				AuditResult.id as sa_rule_result_id ,
				AuditResult.min_score,
				AuditResult.max_score,
				AuditResult.audit_score,
				AuditResult.audit_percentage,
				AuditResult.audit_result,
				AuditResult.audit_type,
				AuditResult.sa_element_order_no,
				AuditResult.is_consider_for_score_card,
				AuditResult.sa_library_element_id,
				Element.sa_element_name,
				library.root as sa_library_id,
				parentlibrary.name as sa_library_name
				from 
				".$customer_schema.".temp_".$this->SessionId." as AuditResult
				join ".$customer_schema.".tbl_sa_library_element_rule_results_temp as AuditTemp on (AuditResult.cdr_service_queue_id=AuditTemp.cdr_service_queue_id and AuditResult.sa_library_element_id=AuditTemp.sa_library_element_id)
				join ".$customer_schema.".tbl_sa_library_elements as Element on (AuditResult.sa_library_element_id=Element.id)
				join ".$customer_schema.".tbl_sa_libraries as library on (Element.sa_library_id=library.id)
				join ".$customer_schema.".tbl_sa_libraries as parentlibrary on (parentlibrary.id=library.root)				
				order by AuditTemp.cdr_GUID,library.root,AuditResult.sa_library_element_id
				 ; 
			";
			 $res = $this->mysqli->query($sql_query);
	 			if ($res->num_rows > 0) {
					// output data of each row					
					$i=0;
					$tempsa_library_id='';
					while($r = $res->fetch_assoc()) {		
						if($tempsa_library_id!=$r['sa_library_id'])
							$i=0;
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['sa_rule_result_id'] =$r['sa_rule_result_id'];						
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['min_score'] =$r['min_score'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['max_score'] =$r['max_score'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['audit_score'] =$r['audit_score'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['audit_percentage'] =$r['audit_percentage'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['audit_result'] =$r['audit_result'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['audit_type'] =$r['audit_type'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['sa_element_order_no'] =$r['sa_element_order_no'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['is_consider_for_score_card'] =$r['is_consider_for_score_card'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['sa_library_element_id'] =$r['sa_library_element_id'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['sa_element_name'] =$this->Cleannonprintable($r['sa_element_name']);
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['sa_library_id'] =$r['sa_library_id'];
						$auditresult[$r['cdr_GUID']]['library'][$r['sa_library_id']]['elements'][$i]['sa_library_name'] =$r['sa_library_name'];
						$i++;
						$tempsa_library_id=$r['sa_library_id'];
					}
				}  
		}		
		return $auditresult;
			  
	 }
	 function getElementResult_old($customer_schema,$cdr_id,$service_queue_id,$sa_library_id)
	 {
		 $auditresult=null;
		if(!empty($cdr_id) && !empty($service_queue_id) && !empty($sa_library_id))
		{	
			$sql_query = " 
			select 
				AuditResult.id as sa_rule_result_id ,
				AuditResult.min_score,
				AuditResult.max_score,
				AuditResult.audit_score,
				AuditResult.audit_percentage,
				AuditResult.audit_result,
				AuditResult.audit_type,
				AuditResult.sa_element_order_no,
				AuditResult.is_consider_for_score_card,
				AuditResult.sa_library_element_id,
				Element.sa_element_name,
				library.root as sa_library_id,
				parentlibrary.name as sa_library_name
				from 
				".$customer_schema.".tbl_sa_library_element_rule_results as AuditResult				
				join ".$customer_schema.".tbl_sa_library_elements as Element on (AuditResult.sa_library_element_id=Element.id)
				join ".$customer_schema.".tbl_sa_libraries as library on (Element.sa_library_id=library.id)
				join ".$customer_schema.".tbl_sa_libraries as parentlibrary on (parentlibrary.id=library.root)
				 where AuditResult.cdr_service_queue_id=$service_queue_id
				 and AuditResult.cdr_id=$cdr_id
				 and library.root=$sa_library_id
				order by library.root,AuditResult.sa_library_element_id
				 ; 
			";
			 $res = $this->mysqli->query($sql_query);
	 			if ($res->num_rows > 0) {
					// output data of each row					
					while($r = $res->fetch_assoc()) {							
						$auditresult[] =$r;						
					}
				}  
		}
		return $auditresult;
			  
	 }
	 /*
	* The function updateCallResult is for update the call analysis result 
	
	*/
	 function updateCallResult($customer_schema,$master_account_no,$cdr_GUIDs,$direction,$searchindex,$cdr_ids)
	 {
		 if(!empty($customer_schema) && !empty($master_account_no) && !empty($cdr_GUIDs) )
		 {
			 $cdr_GUIDs_str=implode(',',array_keys($cdr_GUIDs));
			 $cdr_GUIDs_str=str_replace(',','","', $cdr_GUIDs_str);			 
			 $query='{
					"query": {
						"filtered": {
						   "query": {"match_all": {}},
						   "filter": {
							   "ids": {
								  "values": ["'.$cdr_GUIDs_str.'"									 
								  ]
							   }
						   }
						}
					},
					"fields": 
						[
					   "app_data.client_gender",
					   "app_data.agent_gender",
					   "app_data.agent_clarity",
					   "app_data.client_clarity",
					   "app_data.client_emotion",
						"app_data.agent_emotion",
						"app_data.overall_emotion",
						"app_data.overtalk",
						"app_data.words",
						"app_data.silence",
						 "sentiment"
						
					]
				}';
				$Es = new Elasticsearch();
				$result=$Es->searchdocument($searchindex,'transcript',$query);																		
				$result=json_decode($result,true);
				if(!empty($result) && isset($result['hits']['hits']) && !empty($result['hits']['hits']))
				{ 					
					
					$insertAnalysisDataQuery="Insert into ".$customer_schema.".tbl_sa_call_results (cdr_GUID,agent_clarity,client_clarity,agent_emotion,client_emotion,overall_emotion,overtalk,words,silence,agent_gender,client_gender,sentiment,cdr_id) values";
					$insertAnalysisDataValue="";
					$deleteAnalysisData=array();
						foreach ($result['hits']['hits'] as $cdr)
						{  
							$analysis_data=$cdr["fields"];	
 
							 $client_gender		=(isset($analysis_data["app_data.client_gender"][0])?$analysis_data["app_data.client_gender"][0]:'');
							 $agent_gender		=(isset($analysis_data["app_data.agent_gender"][0])?$analysis_data["app_data.agent_gender"][0]:'');
							 $agent_clarity		=(isset($analysis_data["app_data.agent_clarity"][0])?$analysis_data["app_data.agent_clarity"][0]*100:'null');
							 $client_clarity	=(isset($analysis_data["app_data.client_clarity"][0])?$analysis_data["app_data.client_clarity"][0]*100:'null');
							 $client_emotion	=(isset($analysis_data["app_data.client_emotion"][0])?$analysis_data["app_data.client_emotion"][0]:'');
							 $agent_emotion		=(isset($analysis_data["app_data.agent_emotion"][0])?$analysis_data["app_data.agent_emotion"][0]:'');
							 $overall_emotion	=(isset($analysis_data["app_data.overall_emotion"][0])?$analysis_data["app_data.overall_emotion"][0]:'');
							 $overtalk			=(isset($analysis_data["app_data.overtalk"][0])?$analysis_data["app_data.overtalk"][0]*100:'null');
							 $words				=(isset($analysis_data["app_data.words"][0])?$analysis_data["app_data.words"][0]:'null');
							 $silence			=(isset($analysis_data["app_data.silence"][0])?$analysis_data["app_data.silence"][0]*100:'null');
							 $sentiment			=(isset($analysis_data["sentiment"][0])?$analysis_data["sentiment"][0]:'');
							 $cdr_id				= (isset($cdr_ids[$cdr["_id"]])?$cdr_ids[$cdr["_id"]]:0);
							$insertAnalysisDataValue.="('".$cdr["_id"]."',".$agent_clarity.",".$client_clarity.",'".$agent_emotion."','".$client_emotion."','".$overall_emotion."',".$overtalk.",".$words.",".$silence.",'".$agent_gender."','".$client_gender."','".$sentiment."','".$cdr_id."'),";
							//$deleteAnalysisData[]='"'.$cdr["_id"].'"';
							$deleteAnalysisData[]='"'.$cdr_id.'"';
						} 
							if(!empty($insertAnalysisDataValue))
							{
								$deleteQuery="delete from ".$customer_schema.".tbl_sa_call_results where cdr_id in (".implode(',',$deleteAnalysisData).");";
								
								$res = $this->mysqli->query($deleteQuery);
								if(!$res) {
									printf("Errormessage: %s\n", $this->mysqli->error);					 
								}
								
								$insertAnalysisDataQuery.=substr($insertAnalysisDataValue,0,strlen($insertAnalysisDataValue)-1);
								 
								$res = $this->mysqli->query($insertAnalysisDataQuery);
								if(! $res) {
									printf("Errormessage: %s\n", $this->mysqli->error);					 
								}
							}		
							$this->updateCallType($customer_schema);
							//$this->updateisPyamentCallType($customer_schema);												 
							$this->excludeFailedCallTypeElements($customer_schema);						 
						
				$sql_query = " 
						update ".$customer_schema.".tbl_sa_call_results as CallResult ,
						( 
						 select CallScore.cdr_id,						
									round((	(case when CallScore.audit_score is not null then CallScore.audit_score else 0 end ) +
										(case when CallScore.agent_clarity is not null then CallScore.agent_clarity else 0 end ) +
										(case when CallScore.client_clarity is not null then CallScore.client_clarity else 0 end ) +
										(case when CallScore.overall_emotion is not null then CallScore.overall_emotion else 0 end ) +
										(case when CallScore.overtalk is not null then 100-CallScore.overtalk else 0 end ) +
										(case when CallScore.silence is not null then 100-CallScore.silence else 0 end )
										)/(case when ((case when CallScore.audit_score is not null then 1 else 0 end ) +
										(case when CallScore.agent_clarity is not null then 1 else 0 end ) +
										(case when CallScore.client_clarity is not null then 1 else 0 end ) +
										(case when CallScore.overall_emotion is not null then 1 else 0 end ) +
										(case when CallScore.overtalk is not null then 1 else 0 end ) +
										(case when CallScore.silence is not null then 1 else 0 end ))>0 then  ((case when CallScore.audit_score is not null then 1 else 0 end ) +
										(case when CallScore.agent_clarity is not null then 1 else 0 end ) +
										(case when CallScore.client_clarity is not null then 1 else 0 end ) +
										(case when CallScore.overall_emotion is not null then 1 else 0 end ) +
										(case when CallScore.overtalk is not null then 1 else 0 end ) +
										(case when CallScore.silence is not null then 1 else 0 end )) else 1 end) ,2)  as call_score,
										
									round((	(case when CallScore.audit_score is not null then CallScore.audit_score else 0 end ) +
										(case when CallScore.agent_clarity is not null then CallScore.agent_clarity else 0 end ) +
										(case when CallScore.client_clarity is not null then CallScore.client_clarity else 0 end ) +
										(case when CallScore.agent_emotion is not null then CallScore.agent_emotion else 0 end ) +
										(case when CallScore.overtalk is not null then 100-CallScore.overtalk else 0 end ) +
										(case when CallScore.silence is not null then 100-CallScore.silence else 0 end )
										)/(case when ((case when CallScore.audit_score is not null then 1 else 0 end ) +
										(case when CallScore.agent_clarity is not null then 1 else 0 end ) +
										(case when CallScore.client_clarity is not null then 1 else 0 end ) +
										(case when CallScore.agent_emotion is not null then 1 else 0 end ) +
										(case when CallScore.overtalk is not null then 1 else 0 end ) +
										(case when CallScore.silence is not null then 1 else 0 end ))>0 then ((case when CallScore.audit_score is not null then 1 else 0 end ) +
										(case when CallScore.agent_clarity is not null then 1 else 0 end ) +
										(case when CallScore.client_clarity is not null then 1 else 0 end ) +
										(case when CallScore.agent_emotion is not null then 1 else 0 end ) +
										(case when CallScore.overtalk is not null then 1 else 0 end ) +
										(case when CallScore.silence is not null then 1 else 0 end )) else 1 end),2)  as agent_score,								
										round((	
										(case when CallScore.agent_clarity is not null then CallScore.agent_clarity else 0 end ) +
										(case when CallScore.client_clarity is not null then CallScore.client_clarity else 0 end ) +
										(case when CallScore.overall_emotion is not null then CallScore.overall_emotion else 0 end ) +
										(case when CallScore.overtalk is not null then 100-CallScore.overtalk else 0 end ) +
										(case when CallScore.silence is not null then 100-CallScore.silence else 0 end )
										)/(case when (
										(case when CallScore.agent_clarity is not null then 1 else 0 end ) +
										(case when CallScore.client_clarity is not null then 1 else 0 end ) +
										(case when CallScore.overall_emotion is not null then 1 else 0 end ) +
										(case when CallScore.overtalk is not null then 1 else 0 end ) +
										(case when CallScore.silence is not null then 1 else 0 end ))>0 then  (
										(case when CallScore.agent_clarity is not null then 1 else 0 end ) +
										(case when CallScore.client_clarity is not null then 1 else 0 end ) +
										(case when CallScore.overall_emotion is not null then 1 else 0 end ) +
										(case when CallScore.overtalk is not null then 1 else 0 end ) +
										(case when CallScore.silence is not null then 1 else 0 end )) else 1 end),2)  as acoustics_call_score,
										
									round((	
										(case when CallScore.agent_clarity is not null then CallScore.agent_clarity else 0 end ) +
										(case when CallScore.client_clarity is not null then CallScore.client_clarity else 0 end ) +
										(case when CallScore.agent_emotion is not null then CallScore.agent_emotion else 0 end ) +
										(case when CallScore.overtalk is not null then 100-CallScore.overtalk else 0 end ) +
										(case when CallScore.silence is not null then 100-CallScore.silence else 0 end )
										)/(case when (
										(case when CallScore.agent_clarity is not null then 1 else 0 end ) +
										(case when CallScore.client_clarity is not null then 1 else 0 end ) +
										(case when CallScore.agent_emotion is not null then 1 else 0 end ) +
										(case when CallScore.overtalk is not null then 1 else 0 end ) +
										(case when CallScore.silence is not null then 1 else 0 end ))>0 then (
										(case when CallScore.agent_clarity is not null then 1 else 0 end ) +
										(case when CallScore.client_clarity is not null then 1 else 0 end ) +
										(case when CallScore.agent_emotion is not null then 1 else 0 end ) +
										(case when CallScore.overtalk is not null then 1 else 0 end ) +
										(case when CallScore.silence is not null then 1 else 0 end )) else 1  end) ,2)  as acoustics_agent_score,
										(case when CallScore.audit_score is not null then CallScore.audit_score else 0 end ) as compliance_score,
										(case when CallScore.noofElements>0 then (case when CallScore.is_exception>0 then 'N' else 'Y' end ) else NULL end ) as compliance_audit_result,										
										(case when CallScore.agent_emotion is not null then CallScore.agent_emotion else 0 end ) as agent_emotion_score,
										(case when CallScore.client_emotion is not null then CallScore.overall_emotion else 0 end ) as client_emotion_score,
										(case when CallScore.overall_emotion is not null then CallScore.overall_emotion else 0 end ) as overall_emotion_score
									from 
									  (
										select 
										AuditTemp.cdr_id, 
										CallScore.agent_clarity,  
										CallScore.client_clarity,  
										Performance.rating_point as overall_emotion, 
										AgentPerformance.rating_point as agent_emotion, 
										clientPerformance.rating_point as client_emotion, 
										CallScore.overtalk , 
										CallScore.silence ,
										round(sum( case when AuditResult.is_consider_for_score_card=1 and ElementLibrary.root=4 then AuditResult.audit_score end)/sum( case when AuditResult.is_consider_for_score_card=1 and ElementLibrary.root=4 then  AuditResult.max_score end )*100,2)  as  audit_score,				
										sum(case when AuditResult.audit_result='N' and ElementLibrary.root=4 and AuditResult.is_consider_for_score_card=1 then 1 else 0 end ) as is_exception,
										sum(case when  ElementLibrary.root=4 and AuditResult.is_consider_for_score_card=1 then 1 else 0 end ) as noofElements
										from 
										(select distinct cdr_id,cdr_service_queue_id,sa_library_element_id from ".$customer_schema.".tbl_sa_library_element_rule_results_temp) as AuditTemp									 
										 join ".$customer_schema.".temp_".$this->SessionId." as AuditResult  on (AuditResult.cdr_service_queue_id=AuditTemp.cdr_service_queue_id and AuditResult.sa_library_element_id=AuditTemp.sa_library_element_id and AuditResult.is_consider_for_score_card=1)				 
										  join ".$customer_schema.".tbl_sa_library_elements as Element on (AuditResult.sa_library_element_id=Element.id)
										  join ".$customer_schema.".tbl_sa_libraries as ElementLibrary on (Element.sa_library_id=ElementLibrary.id and ElementLibrary.root=4)
										  join ".$customer_schema.".tbl_sa_call_results as CallScore on (AuditTemp.cdr_id=CallScore.cdr_id)
										  left join ".$customer_schema.".tbl_audit_performance_settings as Performance on (CallScore.overall_emotion=Performance.performance and Performance.performance_analysis_type=4 and  Performance.status=1 and Performance.is_deleted=0)
										  left join ".$customer_schema.".tbl_audit_performance_settings as AgentPerformance on (CallScore.agent_emotion=AgentPerformance.performance and AgentPerformance.performance_analysis_type=4 and  AgentPerformance.status=1 and AgentPerformance.is_deleted=0)
										  left join ".$customer_schema.".tbl_audit_performance_settings as clientPerformance on (CallScore.client_emotion=clientPerformance.performance and clientPerformance.performance_analysis_type=4 and  clientPerformance.status=1 and clientPerformance.is_deleted=0)
										group by AuditTemp.cdr_id	,
										CallScore.agent_clarity,  
										CallScore.client_clarity,  
										Performance.rating_point, 
										AgentPerformance.rating_point, 
										clientPerformance.rating_point, 
										CallScore.overtalk , 
										CallScore.silence  							
									 ) as  CallScore 							 
								) as AuditScore 							 
						set CallResult.call_score=AuditScore.call_score,
						CallResult.agent_score=AuditScore.agent_score,
						CallResult.acoustics_agent_score=AuditScore.acoustics_agent_score,
						CallResult.acoustics_call_score=AuditScore.acoustics_call_score,
						CallResult.compliance_score=AuditScore.compliance_score,
						CallResult.compliance_audit_result=(case when AuditScore.compliance_audit_result='' then null else AuditScore.compliance_audit_result end),
						CallResult.agent_emotion_score=AuditScore.agent_emotion_score,
						CallResult.client_emotion_score=AuditScore.client_emotion_score,
						CallResult.overall_emotion_score=AuditScore.overall_emotion_score					 
						where CallResult.cdr_id=AuditScore.cdr_id;";	 
				 
						if(!$this->mysqli->query($sql_query))
						{
							printf("Errormessage: %s\n", $this->mysqli->error);					 
						 
						}
						 
						
						
				}
				
						
		 }
	 }
	
	/*
	*  function updateCallType for update the call type 	
	*/
	  function updateCallType($customer_schema)
	{
		   
		  if (!empty($customer_schema))
		  {
			  				
			$sql_query = "  
				update ".$customer_schema.".tbl_sa_call_results as CallResult ,
						(
						select  
						(case when sa_element_name='' then 'Others' else sa_element_name end ) as calltype,
						id,
						cdr_id
					from (
						SELECT @row_number:=CASE WHEN @cdr_id=cdr_id THEN @row_number+1 ELSE 1 END AS row_number,@cdr_id:=cdr_id AS cdr_id,id,sa_element_name 
						from 
									(select 												 
										AuditTemp.cdr_id,																					
										 Element.id ,
										 Element.sa_element_name,
										 Element.sa_element_order_no										 
										from 
										".$customer_schema.".tbl_sa_library_element_rule_results_temp as AuditTemp	
									
										join ".$customer_schema.".temp_".$this->SessionId." as AuditResult on (AuditResult.cdr_service_queue_id=AuditTemp.cdr_service_queue_id and AuditResult.sa_library_element_id=AuditTemp.sa_library_element_id)
										join ".$customer_schema.".tbl_sa_library_elements as Element on (AuditResult.sa_library_element_id=Element.id)
										join ".$customer_schema.".tbl_sa_libraries as library on (Element.sa_library_id=library.id)
										join ".$customer_schema.".tbl_sa_libraries as parentlibrary on (parentlibrary.id=library.root)
										 where 					
											library.root=(select var_value from ".$customer_schema.".tbl_sa_general_settings where var_name='calltype_library' and status=1)				 
											and AuditResult.audit_result='Y'
										) as  callType , (SELECT @row_number:=0,@cdr_id:='') AS t
										order by cdr_id,sa_element_order_no
						) as callType
					where row_number=1 
					) as AuditScore
						set CallResult.calltype=AuditScore.calltype,
							CallResult.call_type_element_id=AuditScore.id												
						where CallResult.cdr_id=AuditScore.cdr_id;"; 		

				if(!$this->mysqli->query($sql_query))
						{
							printf("Errormessage: %s\n", $this->mysqli->error);					 
						}
				else
				{
					echo "Call type updated\n</br>";
				}							
					 
				
		  }			
	}
	 
	/*
	*  function updateCallType for update the call type 	
	*/
	  function updateisPyamentCallType($customer_schema)
	{
		    
			
		  if (!empty($customer_schema))
		  {
				$sql_query = "  
				update ".$customer_schema.".tbl_sa_call_results as CallResult ,
						( 
								select
										AuditTemp.cdr_GUID,				
										(case when AuditResult.audit_result='' then 'N' else AuditResult.audit_result end) as audit_result		 			 
										from 
										".$customer_schema.".tbl_sa_library_element_rule_results_temp as AuditTemp										
										join ".$customer_schema.".temp_".$this->SessionId." as AuditResult on (AuditResult.cdr_service_queue_id=AuditTemp.cdr_service_queue_id and AuditResult.sa_library_element_id=AuditTemp.sa_library_element_id)
										join ".$customer_schema.".tbl_sa_library_elements as Element on (AuditResult.sa_library_element_id=Element.id)
										join ".$customer_schema.".tbl_sa_libraries as library on (Element.sa_library_id=library.id)
										join ".$customer_schema.".tbl_sa_libraries as parentlibrary on (parentlibrary.id=library.root)
										 where 					
											Element.id=(select var_value from ".$customer_schema.".tbl_sa_general_settings where var_name='is_payment_call' and status=1)				 											 
							 
						) as AuditScore
						set CallResult.is_payment_call=AuditScore.audit_result						 
						where CallResult.cdr_GUID=AuditScore.cdr_GUID;"; 				 
		
						if(!$this->mysqli->query($sql_query))	
						{
							printf("Errormessage: %s\n", $this->mysqli->error);					 
						}	
		  }			
	}  
	
	/*
		function makeSubRule for prepare subquery for special condition elements
	*/
	
	function makeSubRule($rules)
	{
		if(!empty($rules))
		{
			$query=array();
			 foreach ($rules as $rule)
				{		
						
						if(isset($rule['condition']) && isset($rule['rules']))
						{
													
							$res=$this->makeSubRule($rule['rules']);							 
							if(!empty($res))
							{
								 $query[]='(case when '. implode( $rule['condition'],$res).' then 1 else 0 end )=1 ';
								
							}
						}
						else if(!empty($rule))
						{						
							if($rule['operator']=='equal')	
							$query[]='(case when find_in_set('.$rule['value'].', passresult) then 1 else 0 end )=1 ';
							else if($rule['operator']=='not_equal')
							$query[]='(case when find_in_set('.$rule['value'].', failresult) then 1 else 0 end )=1 ';
						}
				}
				return $query;
		}
	}
	/*
		function makeSpecialConditionQuery for prepare query for special condition elements
	*/
	function makeSpecialConditionQuery($customer_schema,$rule)
	{
			
		if(!empty($customer_schema) && !empty($rule))
		{			
			$final_query='';
						
				if(isset($rule['condition']) && isset($rule['rules']))
				{
					
											
					$res=$this->makeSubRule($rule['rules']);
				 
					if(!empty($res))
					{
						$query='(case when '. implode( $rule['condition'],$res).' then 1 else 0 end )';
						
					}
					if(!empty($query))
					{
					$final_query.="
					select group_concat(cdr_id) as cdr_ids from  (
					select cdr_id, ".$query." as result
							from 
							(
								select cdr_id,group_concat( case when AuditResult.audit_result='N' then sa_library_element_id end) as failresult,
								group_concat( case when AuditResult.audit_result='Y' then sa_library_element_id end) as passresult
								 from ".$customer_schema.".tbl_sa_library_element_rule_results_temp as AuditResult  
								group by cdr_id
								
							) as aa
						) as bb 
						where bb.result=0;";
					}
						
				}
				 
			return $final_query;
		}
	}
	/*
		function getSpecialCondition for list the special condition applicabale elements
	*/
	function getSpecialCondition($customer_schema)
	{
		if(!empty($customer_schema))
		{ 	
			$sql_query = "select sa_library_element_id,rule_special_condition from ".$customer_schema.".tbl_sa_library_element_rules where special_condition_sql_query is not null;";						
			 
			$res_raw = mysqli_query($this->mysqli, $sql_query);
			$res = array();
			if ($res_raw->num_rows > 0) {			
				while($row = mysqli_fetch_assoc($res_raw)) {					
					
					if(!empty($row['rule_special_condition']));
					{
						 $condition=json_decode($row['rule_special_condition'],1);
							 
						 $res[$row['sa_library_element_id']]=$this->makeSpecialConditionQuery($customer_schema,$condition);
					}				
				} 		
				 
				return  $res;
			}
		}	
	}
	/*
		function excludeNotApplicableElements for exclude the not applicabale elements
	*/
	 function excludeNotApplicableElements($customer_schema)
	{
		if(!empty($customer_schema))
		{ 	
		  $query=$this->getSpecialCondition($customer_schema);
		  $result = array();
			if(!empty($query))
			{
				 foreach($query as $element_id=>$query_str)
				 {
						 	$res_raw = mysqli_query($this->mysqli, $query_str);
								$res = array();
								if ($res_raw->num_rows > 0) {			
									while($row = mysqli_fetch_assoc($res_raw)) {					
										
										if(!empty($row['cdr_ids']));
										{
											 $query_delete="Delete from ".$customer_schema.".tbl_sa_library_element_rule_results_temp where sa_library_element_id=$element_id and cdr_id in (".$row['cdr_ids'].");";										
											 mysqli_query($this->mysqli, $query_delete);										 
										}				
									} 		
						 
								}
					}
			}
		}
	}
	/*
	*  function excludeFailedCallTypeElements for remove the call type failed elements 	
	*/
	  function excludeFailedCallTypeElements($customer_schema)
	{
		   
		  if (!empty($customer_schema))
		  {
			// Delete Compliance elements other than call type template 

			$sql_query = "DELETE from ".$customer_schema.".temp_".$this->SessionId." where id in (
						select id from
							(	
								select AuditResult.id from 				 
								(			 
									select AuditResultTemp.cdr_id,libraryElementsetting.id  from 
									(select distinct cdr_GUID,cdr_id from ".$customer_schema.".tbl_sa_library_element_rule_results_temp) as AuditResultTemp
									join ".$customer_schema.".tbl_sa_call_results as CallResult on (AuditResultTemp.cdr_id=CallResult.cdr_id)									
									join ".$customer_schema.".tbl_sa_library_elements as CallTypeElement on (CallResult.call_type_element_id=CallTypeElement.id and CallTypeElement.status=1)																	
								   join ".$customer_schema.".tbl_sa_libraries as  CallTypeElementlibrary on (CallTypeElement.sa_library_id=CallTypeElementlibrary.id)
								   join (
											select Element.id,Element.audit_template_id  from ".$customer_schema.".tbl_sa_libraries as library
											join ".$customer_schema.".tbl_sa_library_elements as Element on (library.id=Element.sa_library_id)
											where library.root =4
											and Element.audit_template_id is not null
                                            and Element.status=1	
									 ) as libraryElementsetting on (CallTypeElement.audit_template_id!=libraryElementsetting.audit_template_id and CallTypeElement.id!=libraryElementsetting.id  and 1=1)
									where CallTypeElementlibrary.root=(select var_value from ".$customer_schema.".tbl_sa_general_settings where var_name='calltype_library' and status=1)
									and CallTypeElement.audit_template_id is not null	
									order by AuditResultTemp.cdr_id 
								) as NAElement
								join (select id,sa_library_element_id,cdr_id,cdr_service_queue_id from ".$customer_schema.".temp_".$this->SessionId." ) as AuditResult on (NAElement.cdr_id=AuditResult.cdr_id and NAElement.id=AuditResult.sa_library_element_id)											 
							) as result
						) ";
 			
 					
					if(!$this->mysqli->query($sql_query))
						{
							printf("Errormessage: %s\n", $this->mysqli->error);					 
						}	
						 
					// Deleting, score not considered call type compliance elements 	 
					$sql_query = "DELETE from ".$customer_schema.".temp_".$this->SessionId." where id in (
						select id from
							(	
								select AuditResult.id	from 				 
								(	
											select AuditResultTemp.cdr_id, libraryElementsetting.id from 
													(select distinct cdr_GUID,cdr_id from ".$customer_schema.".tbl_sa_library_element_rule_results_temp) as AuditResultTemp										
													join ".$customer_schema.".tbl_sa_call_results as CallResult on (AuditResultTemp.cdr_id=CallResult.cdr_id )
													join ( 
															select b.id,b.sa_element_name as calltype from  ".$customer_schema.".tbl_sa_libraries as a 
															join  ".$customer_schema.".tbl_sa_library_elements as b on (a.id=b.sa_library_id) 
															where a.root=(select var_value from ".$customer_schema.".tbl_sa_general_settings where var_name='calltype_library' and status=1)
															and b.is_consider_for_score_card=0 																						 
															) as notConsiderCalltype on (CallResult.call_type_element_id=notConsiderCalltype.id)								
													join (
																   select Element.id from 
																   ".$customer_schema.".tbl_sa_libraries as Elementlibrary
																   join ".$customer_schema.".tbl_sa_library_elements as Element on   (Element.sa_library_id=Elementlibrary.id)				
																	where Elementlibrary.root=4
																) as libraryElementsetting on (1=1)
													 
												) as NAElement			  	
												join  (select id,sa_library_element_id,cdr_id,cdr_service_queue_id from ".$customer_schema.".temp_".$this->SessionId." ) as AuditResult on (NAElement.cdr_id=AuditResult.cdr_id and NAElement.id=AuditResult.sa_library_element_id)											 												
							) as result
						);";			
				if(!$this->mysqli->query($sql_query))
						{
							printf("Errormessage: %s\n", $this->mysqli->error);					 
						}							
					 	 
						// Deleting, othere call type elements 
						
					$sql_query = "DELETE from ".$customer_schema.".temp_".$this->SessionId." where id in (
						select id from
							(	
								select AuditResult.id	from 				 
								(	
											select AuditResultTemp.cdr_id, CalltypeElements.id from 
													(select distinct cdr_GUID,cdr_id from ".$customer_schema.".tbl_sa_library_element_rule_results_temp) as AuditResultTemp										
													join ".$customer_schema.".tbl_sa_call_results as CallResult on (AuditResultTemp.cdr_id=CallResult.cdr_id )													
													join ( 
															select b.id from  ".$customer_schema.".tbl_sa_libraries as a 
															join  ".$customer_schema.".tbl_sa_library_elements as b on (a.id=b.sa_library_id) 
															where a.root=(select var_value from ".$customer_schema.".tbl_sa_general_settings where var_name='calltype_library' and status=1)															
															and b.status=1
														) as CalltypeElements on (CallResult.call_type_element_id!=CalltypeElements.id and 1=1)								
													  
												) as NAElement			  	
												join  (select id,sa_library_element_id,cdr_id,cdr_service_queue_id from ".$customer_schema.".temp_".$this->SessionId." ) as AuditResult on (NAElement.cdr_id=AuditResult.cdr_id and NAElement.id=AuditResult.sa_library_element_id)											 												
							) as result
						);";			
				if(!$this->mysqli->query($sql_query))
						{
							printf("Errormessage: %s\n", $this->mysqli->error);					 
						}							
					 					
		  }			
	}
	 
	 
	 
	/*
	* The function str_replace_json is for replace the word or phrase in json 
	* return String
	*/
	 function str_replace_json($search, $replace, $subject) 
	{
		return json_decode(str_replace($search, $replace, json_encode($subject)), true);
	}	 
	
	/*
	* function getTranscriptByID for get the transcript json text 
	*/
	 
	function getTranscriptByID($custAcct='',$fileid='',$agent_channel=0,$file_location='',$file_key='',$customer_schema='',$customer_id=0, $callback_audio_format='mp3')
	{
	 
		if(!empty($fileid))
		{
			
			$file_path = $this->transcriptBasePath.'/'.$custAcct.'/'.$fileid.'.json';		
			
			$filelist = glob($this->transcriptBasePath.'/'.$custAcct.'/'.$fileid.'*.json');
			if(!empty($filelist) && $filelist[0]!=$file_path)
			{
				exec('mv '.$filelist[0].' '.$file_path, $result);
			}
			
			if(file_exists($file_path))
			{

				$TranscriptJSON=file_get_contents ($file_path);
				$TranscriptJSON=$this->str_replace_json('\"+\"','\"1\"',$TranscriptJSON) ;
				$TranscriptJSON=$this->str_replace_json('\"-\"','\"0\"',$TranscriptJSON) ;
				if(!empty($this->substitionList)) {
                    foreach($this->substitionList as $subKey=>$subVal) {
                        $TranscriptJSON = $this->str_replace_json($subKey, $subVal, $TranscriptJSON);
                    }
                }
				$TranscriptArray=json_decode($TranscriptJSON,1);
				if(!isset($TranscriptArray['app_data']))
				{
					$TranscriptArray=$this->getDocumentOverallanalysis($TranscriptArray,$agent_channel);
				}		 
				if(isset($TranscriptArray['utterances']) &&  !empty($TranscriptArray['utterances']))
				{
					 
					foreach($TranscriptArray['utterances'] as $key1=>$utterances)
					{
						if(!empty($utterances) && !empty($utterances['events']))
						{
							$channle=$utterances['metadata']['channel'];
							
							$eventsTemp=$this->str_replace_json('\"word\"','\"word_'.$channle.'\"',json_encode($utterances['events'])) ;
							 
							$TranscriptArray['utterances'][$key1]['events']=json_decode($eventsTemp,1);							
							
						}
					}
					$TranscriptJSON=json_encode($TranscriptArray);
				}
				
				
			
			
				if(!empty($file_location))
				{
						$d_file_path=dirname($file_location);
						$s_file_path=dirname($file_path);
						$keep_original_file = $this->getCustomerSetting($customer_id, 'keep_original_file');
						$file_action = 0;
						if ($keep_original_file != null && isset($keep_original_file)) {
							if ($keep_original_file) {
								$file_action = 1;
							}
						}
						if ($file_action == 0)
							$this->replaceAudiofile($s_file_path, $d_file_path, $fileid, $file_key, $customer_schema, $callback_audio_format);
						else
							$this->moveAudioFile($s_file_path, $d_file_path, $fileid, $file_key, $file_location, $customer_schema, $callback_audio_format);
				}
				
				return $TranscriptJSON;
			}
		}		
	}	 
	 
	 /* Replace Clean file */
	 
	 function replaceAudiofile($s_file_path,$d_file_path,$fileid,$salt,$customer_schema, $callback_audio_format='mp3')
	 {
		 if(!empty($s_file_path) && !empty($d_file_path) && !empty($salt))
		 {
				//Getting application base directory				   
					$AppBaseDir=dirname(__FILE__);
					$input_file = $s_file_path.'/'.$fileid.'.'.$callback_audio_format;
					$redact_file_path = $d_file_path . '/' . $fileid . '.'.$callback_audio_format;
					$filelist = glob($s_file_path.'/'.$fileid.'*.'.$callback_audio_format);
					$encoded_file = $d_file_path . "/" . $fileid . ".at";

					if(!empty($filelist) && $filelist[0]!=$input_file)
					{
						exec('mv '.$filelist[0].' '.$input_file, $result);
					}
					if (file_exists($input_file)) {
						//decrypt file using crypto utils jar
						$arguments = $salt . ' ' . $input_file . ' ' . $encoded_file . ' ENCRYPT';
						//@chdir($AppBaseDir . "../cdrcomponents");
						exec('java -jar '.$AppBaseDir.'/../cdrcomponents/CryptoUtils.jar ' . $arguments, $result);
						if (file_exists($encoded_file)) {
							exec('rm -rf '.$input_file, $result);
							echo $this->EventDateTime() .'success : Replace file '.$fileid;
							$this->updateRedactDetails($customer_schema, $fileid, $redact_file_path);
						} else {
							echo $this->EventDateTime() .'failed : Replace file '.$fileid;
						}
					} else {
						echo $this->EventDateTime() .'error : Replace file '.$fileid;
						 
					}
		}
		
	 }

	/* Move original and clean file */
	function moveAudioFile($s_file_path, $d_file_path, $fileid, $salt, $file_location, $customer_schema, $callback_audio_format='mp3')
	{
		if (!empty($s_file_path) && !empty($d_file_path) && !empty($salt) && !empty($file_location)) {
			$encoded_file = $d_file_path . "/" . $fileid . ".at";
			$original_d_file_name = basename($file_location);
			if (file_exists($encoded_file)) {
				$original_d_file_path = $d_file_path . "/original";
				if (!file_exists($original_d_file_path) && !is_dir($original_d_file_path)) {
					mkdir($original_d_file_path);
				}

				if (!empty($d_file_path) && !empty($salt)) {
					$original_encoded_file_path = $original_d_file_path . '/' . $fileid . ".at";
					$original_file_path = $original_d_file_path . '/' . $original_d_file_name;
					if (file_exists($encoded_file)) {
						$is_copied = 1;
						if (!file_exists($original_encoded_file_path)) {
							exec('cp ' . $encoded_file . ' ' . $original_encoded_file_path);
							if (file_exists($original_encoded_file_path)) {
								$this->updateOriginalFilePath($customer_schema, $fileid, $original_file_path);
								echo date('Y-m-d H:i:s') . 'success : Copy original file ' . $fileid;
							} else {
								$is_copied = 0;
								echo date('Y-m-d H:i:s') . 'failed : Copy original file ' . $fileid;
							}
						}
						if ($is_copied) {
							$this->replaceAudiofile($s_file_path, $d_file_path, $fileid, $salt, $customer_schema, $callback_audio_format);
						}
					} else {
						echo date('Y-m-d H:i:s') . 'error : Copy original file ' . $fileid;
					}
				}
			}
		}
	}

	/*
	* function getCustomerSetting for get the customer account settings by variable
	*/
	function getCustomerSetting($customer_id, $var_name)
	{
		$var_value = null;
		$sql_query = "select arg_value from tbl_customer_settings where customer_id=$customer_id and arg_name='" . $var_name . "';";
		$res = $this->mysqli->query($sql_query);
		if ($res) {
			$result = array();
			if ($res->num_rows > 0) {
				while ($row = $res->fetch_assoc()) {
					$result[] = $row;
				}
			}
			if(!empty($result))
				$var_value = $result[0]['arg_value'];
		}

		return json_decode($var_value,1);
	}
	
	
	/*
	* function keywordanalyzer for key word analysis 
	*/
	function keywordanalyzer($customer,$cdr_GUIDs,$cdr_ids,$element_id,$agent_channel)
	{
		$keywordAnalysisResult=null;
		if(!empty($customer) && !empty($cdr_GUIDs) && !empty($element_id))
		{

			$keywords=$this->getLibraryElementKeywords($customer["cust_dbname"],$element_id);
			$filter=$this->_getFilter($customer["cust_dbname"],$cdr_GUIDs,$element_id,$agent_channel);

			if(!empty($keywords))

			{
				foreach($keywords as $keyword)
				{
					$keywordexist=array();
					$query=$this->buildESKeywordQuery($keyword['keyword'],$filter,$keyword['keyword_channel'],$customer["agent_channel"],$keyword['keyword_type']);

						if(!empty($query))
						{							 
							$Es = new Elasticsearch();
							//$result=$Es->searchdocument($customer["master_account_no"],'transcript_'.$customer["direction"],$query);																		
							$result=$Es->searchdocument($customer["searchindex"],'transcript',$query);

							echo $this->EventDateTime() .' - get Keyword Result for  '.$keyword['keyword'];
							echo "<br>\n";
							$result=json_decode($result,true);

							if(!empty($result) && isset($result['hits']['hits']) && !empty($result['hits']['hits']))
							{								
								foreach($result['hits']['hits'] as $cdr)
								{									
									if(array_key_exists($cdr['_id'],$cdr_GUIDs))
									{										
										$keywordresult['result']='Y';
                                        $keyword['keyword']=$this->substitute($keyword['keyword']);
										$keywordresult['keyword']=$this->termClean($keyword['keyword']);
										$keywordresult['keyword_type']=$keyword['keyword_type'];
										$keywordresult['keyword_channel']=$keyword['keyword_channel'];	
										$keywordresult['sa_library_element_id']=intval($element_id);
										$keywordresult['sa_library_element_keyword_id']=intval($keyword['id']);
										$keywordAnalysisResult[$cdr['_id']][]=$keywordresult;
										$keywordexist[$cdr['_id']]=1;
									}									 
								}
														
							}	
								 
							/*foreach($cdr_GUIDs  as $GUID=>$service_queue_id)
								{
									 
									if(!isset($keywordexist[$GUID]))
									{

										$keywordresult['result']='N';
                                        $keyword['keyword']=$this->substitute($keyword['keyword']);
										$keywordresult['keyword']=$this->termClean($keyword['keyword']);
										$keywordresult['keyword_type']=$keyword['keyword_type'];	
										$keywordresult['keyword_channel']=$keyword['keyword_channel'];		
										$keywordresult['sa_library_element_id']=$element_id;	
										$keywordresult['sa_library_element_keyword_id']=$keyword['id'];									 								 
										$keywordAnalysisResult[$GUID][]=$keywordresult;										
									}
									
								} */
						}								
				}
			}
		}		
		return $keywordAnalysisResult;
	}
	
	/*
	* function keywordanalyzer for key word analysis 
	*/
	function keywordanalyzer_old($customer,$cdr_GUIDs,$cdr_ids,$element_id)
	{
		$keywordAnalysisResult=null;
		if(!empty($customer) && !empty($cdr_GUIDs) && $element_id)
		{
			$keywords=$this->getLibraryElementKeywords($customer["cust_dbname"],$element_id);
			$filter=$this->_getFilter($customer["cust_dbname"],$cdr_GUIDs,$element_id);	
			
			if(!empty($keywords))
			{
				foreach($keywords as $keyword)
				{
					$query=$this->buildESKeywordQuery($keyword['keyword'],$filter,$keyword['keyword_channel'],$customer["agent_channel"]);				
					 
						if(!empty($query))
						{
							 
							$Es = new Elasticsearch();
							$result=$Es->searchdocument($customer["searchindex"],'transcript',$query);																		
							//$result=$Es->searchdocument($customer["master_account_no"],'transcript_'.$customer["direction"],$query);																		
							echo $this->EventDateTime() .' - get Keyword Result for  '.$keyword['keyword'];
							echo "<br>\n";
							$result=json_decode($result,true);
							if(!empty($result) && isset($result['hits']['hits']) && !empty($result['hits']['hits']))
							{ 
								
								foreach($result['hits']['hits'] as $cdr)
								{
									
									if(array_key_exists($cdr['_id'],$cdr_GUIDs))
									{										
										$keywordAnalysisResult[$cdr_GUIDs[$cdr['_id']]]['keywords'][$keyword['id']]['result']='Y';										
										$keywordAnalysisResult[$cdr_GUIDs[$cdr['_id']]]['keywords'][$keyword['id']]['keyword']=$keyword['keyword'];										
										$keywordAnalysisResult[$cdr_GUIDs[$cdr['_id']]]['keywords'][$keyword['id']]['keyword_type']=$keyword['keyword_type'];
										$keywordAnalysisResult[$cdr_GUIDs[$cdr['_id']]]['keywords'][$keyword['id']]['keyword_channel']=$keyword['keyword_channel'];	
										$keywordAnalysisResult[$cdr_GUIDs[$cdr['_id']]]['keywords'][$keyword['id']]['sa_library_element_id']=$element_id;	
										$keywordAnalysisResult[$cdr_GUIDs[$cdr['_id']]]['cdr_id']=$cdr_ids[$cdr['_id']];
										$keywordAnalysisResult[$cdr_GUIDs[$cdr['_id']]]['cdr_guid']=$cdr['_id'];										
									}
									 
								}
								
							}	
								 
							foreach($cdr_GUIDs  as $GUID=>$service_queue_id)
								{
									 
									if(!isset($keywordAnalysisResult[$service_queue_id]['keywords'][$keyword['id']]))
									{
										
										$keywordAnalysisResult[$service_queue_id]['keywords'][$keyword['id']]['result']='N';	
										$keywordAnalysisResult[$service_queue_id]['keywords'][$keyword['id']]['keyword']=$keyword['keyword'];	
										$keywordAnalysisResult[$service_queue_id]['keywords'][$keyword['id']]['keyword_type']=$keyword['keyword_type'];	
										$keywordAnalysisResult[$service_queue_id]['keywords'][$keyword['id']]['keyword_channel']=$keyword['keyword_channel'];		
										$keywordAnalysisResult[$service_queue_id]['keywords'][$keyword['id']]['sa_library_element_id']=$element_id;
										$keywordAnalysisResult[$service_queue_id]['cdr_id']=$cdr_ids[$GUID];
										$keywordAnalysisResult[$service_queue_id]['cdr_guid']=$GUID;
										
										
									}
									
								}
							 
								
						}
								
				}
			}
			 
		}		
		return $keywordAnalysisResult;
	}
	/*
	* function for getting library elements	
	*/
	function getLibraryElementKeywords($customer_schema,$library_element_id)
	{
		if(!empty($customer_schema) && !empty($library_element_id))
		{
				
		$sql_query = "
			select keyword.id,keyword.keyword,keyword.keyword_channel,keyword_type from  ".$customer_schema.".tbl_sa_library_element_keywords as keyword 		
			where keyword.sa_library_element_id=$library_element_id and	keyword.is_deleted=0;";		
		 
		$res_raw = mysqli_query($this->mysqli, $sql_query);			
		$res = array();
		if ($res_raw->num_rows > 0) {			
			while($row = mysqli_fetch_assoc($res_raw)) {				
				$res[]=$row;
			}
		}		  
		return $res;	
		
		}
	}
	/*
	* function for build Elasticsearch KeywordQuery
	*/
	
	function getESKeywordrule($keywordChannel,$keyword,$agent_channel,$keywordType)
	{
		$rule=null;
		$query=null;
		 
			if($keywordChannel=='wordagent')
				{
					if($keywordType==3) {
                        $rule['bool']['should'][] = $this->_addnearby($keyword, $agent_channel, 1);
                    } else {
                        $rule['bool']['should'][] = $this->_addnearby($keyword, $agent_channel);
                    }
					//$rule['bool']['must'][]['term']['utterances.metadata.channel']['value']=$agent_channel;
					 
				}
				else if($keywordChannel=='wordconsumer')
				{
                    if($keywordType==3) {
                        $rule['bool']['should'][] = $this->_addnearby($keyword, $agent_channel == 0 ? 1 : 0, 1);
                    } else {
                        $rule['bool']['should'][] = $this->_addnearby($keyword, $agent_channel == 0 ? 1 : 0);
                    }
					//$rule['bool']['must'][]['term']['utterances.metadata.channel']['value']=($agent_channel==0?1:0);
					 
				}
				else if($keywordChannel=='wordall')
				{
                    if($keywordType==3) {
                        $rule['bool']['should'][] = $this->_addnearby($keyword, $agent_channel, 1);
                        $rule['bool']['should'][] = $this->_addnearby($keyword, $agent_channel == 0 ? 1 : 0, 1);
                    } else {
                        $rule['bool']['should'][] = $this->_addnearby($keyword, $agent_channel);
                        $rule['bool']['should'][] = $this->_addnearby($keyword, $agent_channel == 0 ? 1 : 0);
                    }
					
					 
			
				} 	
				
		if(!empty($rule))	 	
			{
				$query='{
						"nested": {
							"path": "utterances",
							"query":  
								 '.json_encode($rule).'
								 
						}
					 }';
			}			
			
			return $query;
	}
	/*
	* function for build Elasticsearch KeywordQuery
	*/
	
	function buildESKeywordQuery($keyword,$filter,$keywordChannel,$agent_channel,$keywordType)
	{	 
	
		if(!empty($keyword))
		{		
			$query=$this->getESKeywordrule($keywordChannel,$keyword,$agent_channel,$keywordType);
			if(!empty($query))
			{
					$query=
				'{
					"query":
					{
						"filtered":
						{
							"query":'.$query.(!empty($filter)? ','.$filter:'').'												
						}
					},
					"fields" : []
				}';		
				
				return $query;
			}
			
		}
	
	} 
	function executeQuery($query)
	{		
		if(!empty($query))
		{
			
				$res = $this->mysqli->query($query);
				if(! $res) {
					printf("Errormessage: %s\n", $this->mysqli->error);					 
				}
				else{
					return true;
				}
		}
	}
	
	/*
	* function for update the Keyword Analytic Result to ES
	*/
	function updateSpeechAnalyticKeywordResultToES($customer_schema, $master_account_no,$SpeechAnalyticKeywordResult,$callDates)
	{
		
		if(!empty($SpeechAnalyticKeywordResult))
		{
			//print_r($SpeechAnalyticKeywordResult);exit;
			$CDRKeywordResult=array(); 			
			foreach ($SpeechAnalyticKeywordResult as $keywordresult)
			{ 					
				if(!empty($keywordresult))
				{
					foreach ($keywordresult as $GUID=>$result)
					{
						$CDRKeywordResult[$GUID][]=$result;
					}
					
				}
				  
			}
			$keyworddata=array();
			
			if(!empty($CDRKeywordResult))
			{
					$Es = new Elasticsearch();
					
					$CDRKeywordResultChunk=array_chunk($CDRKeywordResult,100,true);
					
					foreach($CDRKeywordResultChunk as $CDRKeywordResults)
					{
						$param=array();
						foreach($CDRKeywordResults as $GUID=>$data)
						{
							$keywordindex['index']['_index']=$master_account_no;				 
							$keywordindex['index']['_type']='keyword_analysis';
							$keywordindex['index']['_id']=$GUID;	
							$keywordindex['index']['_parent']=$GUID; 
							$records=null;
                            $keyword_result = [];
                            $result = ['y'=>1,'Y'=>1];
                            $keywordType=[1=>1];
                            $channel = ['wordall'=>3,'wordagent'=>2,'wordconsumer'=>1];
							if(!empty($data))
							{
								foreach ($data as $datalevel1)
								{
									if(!empty($datalevel1))
									{
										foreach ($datalevel1 as $datalevel2)
										{
											$records[]=$datalevel2;
                                            if(isset($result[$datalevel2['result']])) {
                                                $keyword_result [] = [
                                                    'result' => 1,
                                                    'keyword' => $datalevel2['keyword'],
                                                    'keyword_type' => isset($keywordType[intval($datalevel2['keyword_type'])]) ? 1 : 0,
                                                    'keyword_channel' => (isset($channel[$datalevel2['keyword_channel']]) ? $channel[$datalevel2['keyword_channel']] : 0),
                                                    'sa_library_element_id' => intval($datalevel2['sa_library_element_id']),
                                                    'sa_library_element_keyword_id' => intval($datalevel2['sa_library_element_keyword_id'])
                                                ];
                                            }
										}
									}
								}
							}
							//$keyworddata['keywords']=$records;
							$keyworddata = [
								'calldate'=>$callDates[$GUID],
								'keywords' => $keyword_result
							];
							$param['body'][]=$keywordindex;
							$param['body'][]=$keyworddata;	
						}
						
					 
						 if(!empty($param))
						 {
									$result = $Es->bulkPostdocument($master_account_no,$param);
										
						 }
														
					}
				
			} 
		}	 
		
	}	
	/*
	* function for update the Speech Analytic Result status
	*/
	function updateSpeechAnalyticKeywordResult($customer_schema, $SpeechAnalyticKeywordResult)			
	{
		 
		
	/*--- Creating Temporary Table for analysis result ---*/
	
	$droptable="DROP TEMPORARY TABLE IF EXISTS ".$customer_schema.".tbl_sa_library_element_keyword_results_temp;";

	$res = $this->mysqli->query($droptable);
				if(! $res) {
					printf("Errormessage: %s\n", $this->mysqli->error);
					 
				} 
		$creattemptable="									
					CREATE temporary TABLE ".$customer_schema.".tbl_sa_library_element_keyword_results_temp (
					`id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Primary Key',
					`sa_library_element_id` INT(11) UNSIGNED NOT NULL,
					 sa_library_element_keyword_id INT(11) UNSIGNED NOT NULL ,	
					`cdr_id` INT(11) UNSIGNED NOT NULL ,
					`cdr_service_queue_id` INT(11) UNSIGNED NOT NULL ,	
					`keyword` VARCHAR(500) NULL DEFAULT NULL COMMENT 'Keyword',
					`keyword_type` TINYINT(1) UNSIGNED NULL DEFAULT '1' COMMENT '1- Positive,0- Negative',
					`keyword_channel` VARCHAR(20) NULL DEFAULT NULL COMMENT 'Keyword channel',
					`audit_result` ENUM('Y','N') NULL DEFAULT NULL	,				
					PRIMARY KEY (`id`)
				)
				COLLATE='latin1_swedish_ci'
				ENGINE=InnoDB;";				 
		$res = $this->mysqli->query($creattemptable);
				if(! $res) {
					printf("Errormessage: %s\n", $this->mysqli->error);
					 
				} 
		
	/*--- prepare result insert query ---*/
		
		if(!empty($SpeechAnalyticKeywordResult))
		{
			$insertquery="Insert into ".$customer_schema.".tbl_sa_library_element_keyword_results_temp (sa_library_element_id,sa_library_element_keyword_id,cdr_id,cdr_service_queue_id,keyword,keyword_type,keyword_channel,audit_result) values ";
			$deleteQuery = "delete from ".$customer_schema.".tbl_sa_library_element_keyword_results
					where cdr_service_queue_id in ";
							
						 
										 
			$cdrids=array();
					 
			$insertvaluequery='';
			$norecords=0;
			 
		 foreach($SpeechAnalyticKeywordResult as $element_id => $keywordDetails)
			{
				if(!empty($keywordDetails))
				{
					foreach($keywordDetails as $service_queue_id => $cdr)
					{
						foreach ($cdr['keywords'] as   $keyword_id=>$result)
						{  
							$insertvaluequery.="($element_id,$keyword_id,".$cdr["cdr_id"].",$service_queue_id,'".$result['keyword']."','".$result['keyword_type']."','".$result['keyword_channel']."','".$result['result']."'),";					
							$norecords++;
							
							if($norecords>=2000)
							{
								$this->executeQuery($insertquery.substr($insertvaluequery,0,strlen($insertvaluequery)-1));
								$insertvaluequery='';
								$norecords=0;
								$this->executeQuery($deleteQuery."(".implode(',',$cdrids).")");
								$cdrids=null;
								
							}
						}		
					 $cdrids[]=$service_queue_id; 
					}
				}
				 
				
			}  
				if(!empty($insertvaluequery))		
				{
						$this->executeQuery($insertquery.substr($insertvaluequery,0,strlen($insertvaluequery)-1));
						$insertvaluequery='';
						$norecords=0;
						if(!empty($cdrids))
						{
							$this->executeQuery($deleteQuery."(".implode(',',$cdrids).")");
							$cdrids=null;
						}
				}
            $now = $this->UTCDateTime();
					$sql_query = "insert into  ".$customer_schema.".tbl_sa_library_element_keyword_results 
								(sa_library_element_id,sa_library_element_keyword_id,cdr_id,cdr_service_queue_id,keyword,keyword_type,keyword_channel,audit_type,audit_result,created_date,created_by,modified_date,modified_by)
								( 
								select 
								sa_library_element_id,
								sa_library_element_keyword_id,
								cdr_id,
								cdr_service_queue_id,
								keyword,
								keyword_type,
								keyword_channel,
								'A' as audit_type,
								audit_result,								
								'$now' as created_date,
								1 as created_by,
								'$now' as modified_date,
								1 as modified_by
								from ".$customer_schema.".tbl_sa_library_element_keyword_results_temp as AuditResult								
								);";
							 
						$res = $this->mysqli->query($sql_query);
						if(! $res) {
							printf("Errormessage: %s\n", $this->mysqli->error);
							 
						} 
				 				
			 
		}	  
		 		 
	}
function getDocumentOverallanalysis($document,$agent_channel=0)
{
  
	$silence=0;
	$overalltalk=array();
	$channel_0_talktime=array();
	$channel_1_talktime=array(); 
	$channel_0_emotion=array();
	$channel_1_emotion=array();
	$overall_emotion=array();	 
	$channel_0_clarity=array();
	$channel_1_clarity=array(); 
	
	$emotion_rating['Negative']['min']=0;
	$emotion_rating['Negative']['max']=24.9;
	$emotion_rating['Worsening']['min']=25;
	$emotion_rating['Worsening']['max']=49.9;
	$emotion_rating['Improving']['min']=50;
	$emotion_rating['Improving']['max']=74.9;	
	$emotion_rating['Positive']['min']=75;
	$emotion_rating['Positive']['max']=100;	
	
	$Positive_emotion[]='mostly positive';
	$Positive_emotion[]='positive';
	
	$negative_emotion[]='negative';
	$negative_emotion[]='mostly negative';
	
	$channel_0_event_talktime=array();
	$channel_0_event_talktime=array();	
	$channel_1_event_talktime=array();
	$channel_1_event_talktime=array();	
	
	$channel_0_word_talktime=array();
	$channel_1_word_talktime=array();	
	$utterances_talktime=array();
	$events_talktime=array();
	$wordsTalktime=array();
	$words=0;
	$result=array();	
	$channel_0_gender=array();
	$channel_1_gender=array();
	if(!empty($document))
	{
		$result['agent_channel']=$agent_channel;
		$result['client_channel']=($agent_channel==1?0:1);
	
		foreach($document['utterances'] as $utterances)
		{
			if(!empty($utterances) && $utterances['confidence']>0)
			{
				if($utterances['metadata']['channel']==0 )
				{ 
						$start = $utterances['start'];
						$end = $utterances['end'];
						$channel_0_talktime[]=$start;
						$channel_0_talktime[]=$end;
						$utterances_talktime[]=$start;  
						$utterances_talktime[]=$end;					 
							 
					 if(!empty($utterances['events']))
					 { 
						    $first = reset($utterances['events']);
							$last = end($utterances['events']);
							$start = $first['start'];
							$end = $last['end'];
							$channel_0_event_talktime[]=$start;
							$channel_0_event_talktime[]=$end ;							
						   $events_talktime[]=$start;
						   $events_talktime[]=$end;
						
						 foreach ($utterances['events'] as $key=>$event)
						 {  
							$start = $event['start'];
							$end = $event['end'];
							$channel_0_word_talktime[]=$start ;
							$channel_0_word_talktime[]=$end;
                             $wordsTalktime[]=$start;
                             $wordsTalktime[]=$end;
							$words++;
						 }  
					 }   
					/* -- emotion --*/
					if(isset($utterances['emotion'])  )
					$channel_0_emotion[$utterances['emotion']][]=1;
				
					/*-- clarity --*/					 
					$channel_0_clarity[]=$utterances['confidence']; 
				}
				if($utterances['metadata']['channel']==1 )
				{						 
						   $start = $utterances['start'];
							$end = $utterances['end'];
							$channel_1_talktime[]=$start;
							$channel_1_talktime[]=$end ;
						$utterances_talktime[]=$start;  
						$utterances_talktime[]=$end;
					 
					 if(!empty($utterances['events']))
					 {
						    $first = reset($utterances['events']);
							$last = end($utterances['events']);
							$start = $first['start'];
							$end = $last['end'];
							$channel_1_event_talktime[]=$start;
							$channel_1_event_talktime[]=$end ;	
							$events_talktime[]=$start;
							$events_talktime[]=$end;							
							
						 foreach ($utterances['events'] as $event)
						 {
							$start = $event['start'];
							$end = $event['end'];
							$channel_1_word_talktime[]=$start;
							$channel_1_word_talktime[]=$end;
                           $wordsTalktime[]=$start;
                           $wordsTalktime[]=$end;
							$words++;							
						 }
					 }   
					 /* -- emotion --*/
					 if(isset($utterances['emotion']) )
					$channel_1_emotion[$utterances['emotion']][]=1;
				 				
					/*-- clarity --*/					 
					$channel_1_clarity[]=$utterances['confidence']; 
				}  
					/* -- overallemotion --*/
					if(isset($utterances['emotion']))
					$overall_emotion[$utterances['emotion']][]=1; 
			}
		}
		 /* -- Overtalk --*/
		 /* $overtalk=array(); 
		 $channel_totaltalktime=array(); 		 
		 foreach ($channel_0_talktime as $key0=>$channel_0)
		 {			 
			 if($key0%2)
			 {			 
				$channel_0_s=$channel_0_talktime[$key0-1];
				$channel_0_e=$channel_0_talktime[$key0];	
				$channel_totaltalktime[]=$channel_0_e-$channel_0_s;
				
					 foreach ($channel_1_talktime as $key1=>$channel_1)
					{
						if($key1%2)
						{							
								$channel_1_s=$channel_1_talktime[$key1-1];
								$channel_1_e=$channel_1_talktime[$key1];	 
						  if($key0==1)						
							{	
								$channel_totaltalktime[]=$channel_1_e-$channel_1_s;
							} 
							if($channel_1_s>=$channel_0_s && $channel_1_e<=$channel_0_e)
							{
								$overtalk[]=$channel_1_e-$channel_1_s; 	 
							}
							else if($channel_1_s>=$channel_0_s && $channel_1_e>=$channel_0_e && $channel_1_s<=$channel_0_e)
							{ 
								 $overtalk[]=$channel_0_e-$channel_1_s;		 
							}
							else if($channel_1_s<=$channel_0_s &&  $channel_1_e<=$channel_0_e && $channel_1_e>=$channel_0_s)
							{
								$overtalk[]=$channel_1_e-$channel_0_s; 
							}
							else if($channel_1_s<=$channel_0_s && $channel_1_e>=$channel_0_e)
							{
								$overtalk[]=$channel_0_e-$channel_0_s; 
							}	 
						}	 
					}	 
			 } 
		 } 		 		 
		 $result['overtalk']=round(array_sum($overtalk)/array_sum($channel_totaltalktime),3);
		 */
		 
	    /* -- Overtalk --*/
		 $overtalk=array(); 
		 $channel_totaltalktime=array(); 		 
		 $i=0;
		 foreach ($channel_0_word_talktime as $key0=>$channel_0)
		 {			 
			 if($key0%2)
			 {			 
				$channel_0_s=$channel_0_word_talktime[$key0-1];
				$channel_0_e=$channel_0_word_talktime[$key0];
				$channel_totaltalktime[]=$channel_0_e-$channel_0_s;
				$overtalk_tmp[$i]['start']=$channel_0_s;
				$overtalk_tmp[$i]['end']=$channel_0_e;
				$overtalk_is=0;
					 foreach ($channel_1_word_talktime as $key1=>$channel_1)
					{
						if($key1%2)
						{							
								$channel_1_s=$channel_1_word_talktime[$key1-1];
								$channel_1_e=$channel_1_word_talktime[$key1];
						  if($key0==1)						
							{	
								$channel_totaltalktime[]=$channel_1_e-$channel_1_s;
							} 
							if($channel_1_s <= $channel_0_s && $channel_1_s <=$channel_0_e && $channel_1_e >= $channel_0_s && $channel_1_e <= $channel_0_e)
							{
								$overtalk_tmp[$i]['start']=$channel_1_s;
								$overtalk_tmp[$i]['end']=$channel_1_e;
								$overtalk_is=1;
							}
							if( $channel_1_s >= $channel_0_s && $channel_1_s <= $channel_0_e && $channel_1_e >= $channel_0_s && $channel_1_e <= $channel_0_e)
							{
								$overtalk_tmp[$i]['end']=$channel_1_e;
								$overtalk_is=1;
							}	
							if( $channel_1_s >= $channel_0_s && $channel_1_s <= $channel_0_e && $channel_1_e >= $channel_0_s && $channel_1_e >= $channel_0_e)
							{
								$overtalk_tmp[$i]['end']=$channel_1_e;
								$overtalk_is=1;
							}
						}	 
					}	
					
					if($overtalk_is==1)
					{
						
						$overtalk[]=$overtalk_tmp[$i]['end']-$overtalk_tmp[$i]['start'];
					}
					
						$i++;
			 } 
		 } 		 		 
				reset($wordsTalktime);
                $start_=current($wordsTalktime);
                $end_=end($wordsTalktime);
				if(($end_ - $start_)!=0) {
					$result['overtalk'] = round(array_sum($overtalk) / ($end_ - $start_), 3);
				} else {
					$result['overtalk'] = 0;
				}
		 /* -- Silence --*/  		 
		 $silence=array();	
		 $silence_time=array();
		if(!empty($utterances_talktime)) 
		 {
			 foreach ($utterances_talktime as $key0=>$channel_0)
				{			 
					 if($key0%2)
					 {
							$utterence_s=($utterances_talktime[$key0-1]);
						 	$utterence_e=($utterances_talktime[$key0]);
							$utterence_ns=(isset($utterances_talktime[$key0+1])?$utterances_talktime[$key0+1]:$utterence_e);	
							
							$event_s=($events_talktime[$key0-1]);
						 	$event_e=($events_talktime[$key0]);
							$event_ns=(isset($events_talktime[$key0+1])?$events_talktime[$key0+1]:$event_e);
							if(round($event_ns)>round($event_e))
							{ 
								$s=round($event_ns-$event_e);
								if($s>0)
								{
									$silence[]=$s;
									$silence_time[(string)$event_e]=$s;
								}
								
							}
							 
					 }
				}
		 } 
			$talk_duration=end($utterances_talktime)- reset($utterances_talktime);
			$silence_duration=array_sum($silence);
			if($talk_duration>0)
				$silence=round(($silence_duration/($talk_duration)),3);
		  $result['silence']=$silence;		  
		  
		 /* -- Emotion--*/
	 
		 $channel_0_emotion_range='';
		 $channel_1_emotion_range='';
		 $overall_emotion_range='';
		 
		 if(!empty($channel_0_emotion))
		 {
			 $Positive=0;
			  $negative=0;
			 foreach ($channel_0_emotion as $emotion=>$value)
			 {				
						 
				if(in_array(strtolower($emotion),$Positive_emotion))
					$Positive=$Positive+array_sum($value);
				else if(in_array(strtolower($emotion),$negative_emotion))
					$negative=$negative+array_sum($value);	 
			 } 
			 	$total=$Positive+$negative;
				if($total>0)
					$emotion_per=round($Positive/$total*100,2);
				else
					$emotion_per=100;
				foreach ($emotion_rating as $emotion=>$rating)
				{
					if($emotion_per>=$rating['min'] && $emotion_per<=$rating['max'])
					{
						$channel_0_emotion_range=$emotion;
						break;
					}
				}
		 }
		 if(!empty($channel_1_emotion))
		 {
			 
			  $Positive=0;
			  $negative=0;
			 foreach ($channel_1_emotion as $emotion=>$value)
			 {				
				 
				if(in_array(strtolower($emotion),$Positive_emotion))
					$Positive=$Positive+array_sum($value);
				else if(in_array(strtolower($emotion),$negative_emotion))
					$negative=$negative+array_sum($value);		
			 
			 }
			 
			 	$total=$Positive+$negative;
				if($total>0)
					$emotion_per=round($Positive/$total*100,2);
				else
					$emotion_per=100;
				foreach ($emotion_rating as $emotion=>$rating)
				{
					if($emotion_per>=$rating['min'] && $emotion_per<=$rating['max'])
					{
						$channel_1_emotion_range=$emotion;
						break;
					}
				}
		 }
		  if(!empty($overall_emotion))
		 {
			 
			  $Positive=0;
			  $negative=0;
			 foreach ($overall_emotion as $emotion=>$value)
			 {				
			 
				if(in_array(strtolower($emotion),$Positive_emotion))
					$Positive=$Positive+array_sum($value);
				else if(in_array(strtolower($emotion),$negative_emotion))
					$negative=$negative+array_sum($value);		
			 
			 }
			 
			 	$total=$Positive+$negative;
				if($total>0)
					$emotion_per=round($Positive/$total*100,2);
				else
					$emotion_per=100;
				foreach ($emotion_rating as $emotion=>$rating)
				{
					if($emotion_per>=$rating['min'] && $emotion_per<=$rating['max'])
					{
						$overall_emotion_range=$emotion;
						break;
					}
				}
		 }
		 if($agent_channel==1)
		 {
			  $result['agent_emotion']= $channel_1_emotion_range;
			  $result['client_emotion']= $channel_0_emotion_range;
		 }
		 else
		 {
			  $result['agent_emotion']= $channel_0_emotion_range;
			  $result['client_emotion']= $channel_1_emotion_range;
		 } 
		  $result['overall_emotion']= $overall_emotion_range;
		 
	/*--- voice clarity --*/
		 $channel_0_clarity_per=0;
		 $channel_1_clarity_per=0;
		 if(!empty($channel_0_clarity))
		 {
			 $sum=array_sum($channel_0_clarity);
			 $count=sizeof($channel_0_clarity);
			 $channel_0_clarity_per=round($sum/$count,3);
		 }  
		 if(!empty($channel_1_clarity))
		 {
			 $sum=array_sum($channel_1_clarity);
			 $count=sizeof($channel_1_clarity);
			 $channel_1_clarity_per=round($sum/$count,3);
		 }
		  if($agent_channel==1)
		 {
			  $result['agent_clarity']= $channel_1_clarity_per;
			  $result['client_clarity']= $channel_0_clarity_per;
		 }
		 else
		 {
			  $result['agent_clarity']= $channel_0_clarity_per;
			  $result['client_clarity']= $channel_1_clarity_per;
		 } 
		  
		 /*-- Call duration --*/
		 
			$last_event=end($document['utterances']);
			$callduration_sec=$last_event['end'];
			$result['duration']=gmdate('H:i:s', $callduration_sec);
		 /*-- Words --*/			
			$result['words']=$words; 
		  if(!empty($result))
		  {
			  $document['app_data']=$result;
		  }
	}
	return $document;	
}		
 /*
 */
 function createReadonlyIndex($customer,$cdr_GUIDs=null)
 {
	 if(!empty($customer)&& !empty($cdr_GUIDs))
	 {
		 $Es = new Elasticsearch();	
		 $SrcIndex=$customer["master_account_no"];
		 $DestIndex=$customer["master_account_no"].'_readonly';
		 $this->deleteReadonlyIndex($DestIndex);
			$filterIds=$this->_getIDFilter($cdr_GUIDs);				 
			$filter='"filter":{	
								"bool": {
									"must": [
										'.(!empty($filterIds)?$filterIds:'').(!empty($fieldFilter)?','.$fieldFilter:'').'
										]
										}								 
								}';	
				$searchquery='{"query": {
								"filtered": { 
									"query": {"match_all": {}},
									'.$filter.'									
									 }},
								 "fields": ["*"], "_source": true
									 }';		
			// Copy CDR 
			$Es->copyIndex($SrcIndex.'/cdr', $DestIndex,$searchquery);
			// Copy transcript 
			$Es->copyIndex($SrcIndex.'/transcript', $DestIndex,$searchquery);				
		  
		 return $DestIndex;
	 }
	
 }
 /*
  * function deleteReadonlyIndex for delete the temporary created index
 */
 
 function deleteReadonlyIndex($index)
 {
	  $Es = new Elasticsearch();	
	  if($Es->checkIndex($index))
		{
			 $Es->deleteIndex($index);	
		}	
 }

    /**
     * Get UTC date for record create and modify events
     * @param string $format
     * @param string $dateTime
     * @return string
     */
    public function UTCDateTime($format = 'Y-m-d H:i:s', $dateTime='now'){
        $dt = new DateTime($dateTime, new DateTimeZone('UTC'));
        return $dt->format($format);
    }

    /**
     * Display Event date with user own zone format
     * @param string $dateTime
     * @return string
     */
    public function EventDateTime($dateTime = 'now'){
        $dt = new DateTime($dateTime, new DateTimeZone($this->defaultTimeZone));
        return $dt->format($this->datetimeFormat);
    }
}
?>
