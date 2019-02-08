# get randomized list of sample ids
vcf-query -l 10ADSP_ATLAS_in_consensus_NSSS.vcf | shuf > samples_shuffled.txt
# split list of sample ids to files of 1025 lines (20%)
split -l 1025 samples_shuffled.txt sampleSplit
#subset vcf file to contain only samples from the 1025 split list 
bcftools view -S sampleSplitaa 10ADSP_ATLAS_in_consensus_NSSS.vcf > 10ADSP_ATLAS_in_consensus_NSSS_testSplit.vcf 
#subset the vcf file to contain only sample that are not in the 1025 line split list
bcftools view -S ^sampleSplitaa 10ADSP_ATLAS_in_consensus_NSSS.vcf > 10ADSP_ATLAS_in_consensus_NSSS_trainSplit.vcf 
#subset the labels file to contain only the ids that are in the test split sample id list
awk -F',' 'NR==FNR{c[$1]++;next};c[$1] > 0' sampleSplitaa labels.txt > labels_test.txt
#merge the sample id list split files 1026-5128 (5 files)
cat sampleSplitab sampleSplitac sampleSplitad sampleSplitae sampleSplitaf >> sampleSplitrain.txt
#generate trainingset by using only lines from labels.txt that are in sampleSplittrain
awk -F',' 'NR==FNR{c[$1]++;next};c[$1] > 0' sampleSplitrain.txt labels.txt > labels_train.txt
#forgot headers:
sed -i '1 i\samples,label' labels_train.txt
sed -i '1 i\samples,label' labels_test.txt

