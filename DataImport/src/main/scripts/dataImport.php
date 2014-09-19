<?php
 
date_default_timezone_set('America/Denver');
//ini_set('max_execution_time', 360000); // unit: second; default: 30 seconds.
//ini_set('memory_limit', '2048M'); // may need a large buffer for

$curr_dir = '/home/hadoop/lucas/';


/*
 * CPI is published about in 19th, one month older.
 * e.g. today is Sept 20th, then we can get Aug 1st CPI
 */
 
$date = date("Y-m-d");
//$newdate = strtotime ( '-1 month' , strtotime ( $date ) ) ;
$newdate = date ( 'Y-m' , strtotime ( $date ) );
$newdate = $newdate . '-01';
//echo "Check: {$newdate}\n";
 
$url = 'https://research.stlouisfed.org/fred2/series/CPIAUCSL/downloaddata';
$gdp_url = 'https://research.stlouisfed.org/fred2/series/GDP/downloaddata';


//$newdate = "2014-07-01";
 
function download($url, $path, $date, $attribute) {
    $fp = fopen($path, 'w');
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, false);
    curl_setopt($ch, CURLOPT_BINARYTRANSFER, true);
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 30);
    curl_setopt($ch, CURLOPT_FILE, $fp);
	$cpi_pattern = '/CPI_/';
	$gdp_pattern = '/GDP_/';
	if(preg_match($cpi_pattern, $attribute, $matches)) {
		$params = 'form%5Bnative_frequency%5D=Monthly&form%5Bdownload_data_1%5D=&form%5Bunits%5D=lin&form%5Bfrequency%5D=Monthly&form%5Baggregation%5D=Average&form%5Bobs_start_date%5D=1947-01-01&form%5Bobs_end_date%5D=' . $date . '&form%5Bfile_format%5D=csv';
	}
	else if (preg_match($gdp_pattern, $attribute, $matches)) {
		$params = 'form%5Bnative_frequency%5D=Quarterly&form%5Bdownload_data_1%5D=&form%5Bunits%5D=lin&form%5Bfrequency%5D=Quarterly&form%5Baggregation%5D=Average&form%5Bobs_start_date%5D=1947-01-01&form%5Bobs_end_date%5D=' . $date . '&form%5Bfile_format%5D=csv';
    }
	//echo $date;
	curl_setopt($ch, CURLOPT_POSTFIELDS, $params);
    curl_exec($ch);
    curl_close($ch);
    fclose($fp);
 
    if (filesize($path) > 0) {
        return true;
    }
}

function isReport($path) {
	try {
		$file = file_get_contents($path, true);
		$pattern = '/<!DOCTYPE html>/';
		
		if(preg_match($pattern, $file, $matches)) { // illegal file
			return false;//getOldFile($newdate);
		}
		return true;
	
	} catch (Exception $e) {
		 echo "Report open problem: {$path}\n{$e}\n";
	}
	
}
 
function mail_me($message) {
	$to      = 'nobody@example.com';
	$subject = 'AutoDownload CPI Error';

	$headers = 'From: ysong@insidesales.com' . "\r\n" .
		'Reply-To: ysong@insidesales.com' . "\r\n" .
		'X-Mailer: PHP/' . phpversion();

	mail($to, $subject, $message, $headers);
}

function write_log($msg) {
	$logfile = DEFAULT_LOG;
	$error_dir = './autoDownload_errors.log';
	$date = date('d.m.Y h:i:s');
	//$msg = "check, check ...";
	$log = "Time: {$date}  {$msg}\n";
	
	error_log($log, 3, $error_dir);
	
	mail_me($log);
}

 
function getOldFile($url, $date, $attribute) {
	//echo "double check: {$date}\n";
	$newdate = strtotime ( '-1 month' , strtotime ( $date ) ) ;
	$newdate = date('Y-m-d', $newdate);
	$newPath = "{$attribute}{$newdate}.csv";
	//echo "OldFile: {$newdate} and {$newPath}\n";
	/*
	 * if the old date file exists, then we use it. Otherwise, download it.
	 * if the download file is legal, then use it. Otherwise, check the older date file.
	 */
	
	while(!file_exists($newPath) || !isReport($newPath)) {
		if(download($url, $newPath, $newdate,$attribute)) {
			//echo "{$newPath} Download complete!\n";
			if(isReport($newPath)){
				break;
			}
			unlink($newPath);
			//echo "Not right, Deleted: {$newPath}\n";
		}
		$newdate = strtotime ( '-1 month' , strtotime ( $newdate ) ) ;
		$newdate = date('Y-m-d', $newdate);
		$newPath = "{$attribute}{$newdate}.csv";
		//echo "Next OldFile: {$newdate} and {$newPath}\n";
	}
	
	//echo "Return OldFile: {$newPath}\n";
	return $newPath;
} 

$opts = "c:";
$opts .= "g:";
$options = getopt($opts);

$d = date("Y-m-d H:i:s");
echo "start Time {$d}\n";
 
/*
 * CPI download
 */
$attribute = "{$curr_dir}CPI_";
$path = "{$attribute}{$newdate}.csv";
echo "test: {$path}\n";


if (download($url, $path, $newdate, $attribute)) {
	
	if(!isReport($path)){
		unlink($path);
		//echo "Deleted: {$path}\n";
		$path = getOldFile($url,$newdate,$attribute);
	}
	
	echo "\nBefore run java, {$path}\n";
	
	if($options["c"] == "cpi_data_test") {
		echo "test cpi\n";
		exec("java -jar {$curr_dir}DataImport.jar cpi_data_test {$path}");
	}
	else if($options["c"] == "cpi_data") {
		echo "real cpi\n";
		exec("java -jar DataImport.jar cpi_data {$path}");
	}
}

/*
 * GDP download
 */
$attribute = "{$curr_dir}GDP_";
$path = "{$attribute}{$newdate}.csv";
if (download($gdp_url, $path, $newdate, $attribute)) {
    //echo "{$path} Download complete!\n";
	if(!isReport($path)){
		unlink($path);
		//echo "Deleted: {$path}\n";
		$path = getOldFile($gdp_url,$newdate,$attribute);
	}
	
	echo "\nBefore run java, {$path}\n";
	
	if($options["g"] == "gdp_data_test") {
		echo "test gdp\n";
		exec("java -jar {$curr_dir}DataImport.jar gdp_data_test {$path}");
	}
	else if($options["g"] == "gdp_data") {
		echo "real gdp\n";
		exec("java -jar DataImport.jar gdp_data {$path}");
	}
	
}

?>