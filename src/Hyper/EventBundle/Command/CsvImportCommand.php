<?php
namespace Hyper\EventBundle\Command;

use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

use Hyper\EventBundle\Document\Person;
use Hyper\EventBundle\Document\Transaction;
use Hyper\EventBundle\Annotations\CsvMetaReader;

class CsvImportCommand extends ContainerAwareCommand
{
    protected function configure()
    {
        $this
            ->setName('csv:import')
            ->setDescription('load csv parse to JSON and store to S3')
            ->addOption(
                'file',
                null,
                InputOption::VALUE_REQUIRED,
                'CSV file path to import'
            )
        ;
    }
    
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $file = $input->getOption('file');
        $contents = $this->parseCsvContent($file);
        
        /*
        if($hoursAgo && is_numeric($hoursAgo)){
            $storageController = $this->getContainer()->get('hyper_event.storage_controller');
            $storageController->pushMemcachedToMongoDB($hoursAgo);
        }else{
            echo "invalid option value";
        }
        */
        
    }
    
    protected function parseCsvContent($csvFile)
    {
        // 2015-08-06 - Ding Dong : Added variables to store process information
        $content_lines = array();
        $process_results = array();
        $success_count = 0;
        $fail_count = 0;
        $process_start_datetime = time();

        $csvMetaReader = new CsvMetaReader();
        $personCsvMongoDbIndex = $csvMetaReader->csvMongoDbIndex('\Hyper\EventBundle\Document\Person');
        $transactionCsvMongoDbIndex = $csvMetaReader->csvMongoDbIndex('\Hyper\EventBundle\Document\Transaction');
        $csvMongoDbIndex = array_merge($personCsvMongoDbIndex,$transactionCsvMongoDbIndex);
        $content = array();
        
        $storageController = $this->getContainer()->get('hyper_event.storage_controller_v4');
        
        $amazonBaseURL = $storageController->getAmazonBaseURL();
        $rootDir = $storageController->get('kernel')->getRootDir();// '/var/www/html/projects/event_tracking/app'
        $rawLogDir = $rootDir. '/../web/raw_event';
        $s3FolderMappping = $storageController->getS3FolderMapping();
        $supportProvider = $storageController->getPostBackProviders();
        $providerId = 0;
        if($providerId!==null && array_key_exists($providerId,$supportProvider)) {
            $storageController->postBackProvider = $supportProvider[$providerId];
        }
        
        if (($handle = fopen($csvFile, "r")) !== false) {
            $i = 0;
            $header = array();
            while(($row = fgetcsv($handle)) !== false) {
                if($i == 0){
                    $header = $row;
                } else {
                    $contentIndex = $i-1;
                    foreach ($header as $index => $columnName) {
                       $mongoIndex = array_search(strtolower($columnName),$csvMongoDbIndex);
                       if ($mongoIndex) {
                            $content[$contentIndex][$mongoIndex] = $row[$index];
                       }
                    }
                    // 2015-08-06 - Ding Dong : Added to include original content line
                    $content_lines[$contentIndex]["content_raw"] = $content[$contentIndex];

                    $rawContent = json_encode($content[$contentIndex]);

                    // 2015-08-06 - Ding Dong  : Added to include JSON version of content line
                    //$content_lines[$contentIndex]["content_json"] = $rawContent;

                    $result = $storageController->storeEventS3(
                        $rawContent,
                        $content[$contentIndex],
                        $amazonBaseURL,
                        $rawLogDir,
                        $s3FolderMappping

                    );

                    // 2015-08-06 - Ding Dong : Added condition block added to check if the file was created in S3 bucket and indicate status
                    if (null != $result) {
                        // File creation successful
                        $content_lines[$contentIndex]["s3_path"] = $result;
                        $content_lines[$contentIndex]["status"] = "Success";
                        $success_count++;
                    } else {
                        // File creation failed
                        $content_lines[$contentIndex]["s3_path"] ="";
                        $content_lines[$contentIndex]["status"] = "Failed";
                        $fail_count++;
                    }
                }
                $i++;
            }
        }

        // 2015-08-06 - Ding Dong : Added block to consolidate process information for display
        // START
        $process_end_datetime = time();
        $timeDiff = $process_start_datetime - $process_end_datetime;

        $process_results["content_count"] = $i - 1;
        $process_results["processed_count"] = count($content_lines);
        $process_results["success_count"] = $success_count;
        $process_results["fail_count"] = $success_count;
        $process_results["content_lines"] = $content_lines;
        $process_results["start_datetime"] = $process_start_datetime;
        $process_results["end_datetime"] = $process_end_datetime;
        $process_results["elapse_time"] = $timeDiff;

        $process_results_json = json_encode($process_results);

        echo $process_results_json;
        // END


        //return $content;
    }
    
}