#!/bin/sh

START_DIR=`pwd`

# Cleanup stuff from a previous run
rm -rf apache-maven-3.9.9
rm -rf install.log
rm -f outputs.txt

chmod u+x hello-aurora-dsql/*.sh
chmod u+x bin/*

# Install Java 21
echo "Installing Java..."
sudo yum install -q -y java-21-amazon-corretto-devel >> install.log
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto.x86_64/' >> $HOME/.bashrc

# Install Maven
echo "Installing Maven..."
wget -q https://dlcdn.apache.org/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz
tar xzf apache-maven-3.9.9-bin.tar.gz >> install.log
echo 'export PATH=$PATH:$HOME/aws-summit-dsql-workshop-main/apache-maven-3.9.9/bin' >> $HOME/.bashrc
rm apache-maven-3.9.9-bin.tar.gz

# Download and install Aurora Java SDK
echo "Installing Aurora DSQL Java SDK..."
apache-maven-3.9.9/bin/mvn -q install:install-file -Dfile=./rewards-backend/lib/AwsJavaSdk-Dsql-2.0.jar -DgroupId=software.amazon.awssdk.services  -DartifactId=dsql -Dversion=1.0 -Dpackaging=jar

# Create an S3 bucket
echo "Creating Rewards App code S3 bucket..."
AWS_ACCT=`aws sts get-caller-identity --query Account --output text`
BUCKET_NAME="code-$AWS_ACCT-$AWS_REGION"

if [ $AWS_REGION = 'us-east-1' ]
then
    aws s3api create-bucket --bucket $BUCKET_NAME >> install.log
    TEMPLATE_URL="https://$BUCKET_NAME.s3.amazonaws.com/rewards-app-stack.yml"
else 
    aws s3api create-bucket --bucket $BUCKET_NAME --create-bucket-configuration LocationConstraint=$AWS_REGION >> install.log
    TEMPLATE_URL="https://$BUCKET_NAME.s3.$AWS_REGION.amazonaws.com/rewards-app-stack.yml"
fi

# Push code to the bucket
echo "Pushing Rewards App to S3..."
aws s3 cp rewards-backend/src/main/cfn/rewards-app-stack.yml "s3://$BUCKET_NAME/" >> install.log
cd rewards-backend
$START_DIR/apache-maven-3.9.9/bin/mvn -q package
cd ..
aws s3 cp rewards-backend/target/rewards-points-1.0.jar "s3://$BUCKET_NAME/" >> install.log
aws s3 cp rewards-backend/target/rewards-points-1.0-layer.zip "s3://$BUCKET_NAME/" >> install.log

echo
echo "Code Bucket:       $BUCKET_NAME"
echo "App Template URL:  $TEMPLATE_URL"
echo
echo "CodeBucketName:       $BUCKET_NAME" >> outputs.txt
echo "Template S3 URL:      $TEMPLATE_URL" >> outputs.txt


# Set some useful environment variables
echo "Updating shell profile..."
echo 'export HELLO_HOME=$HOME/hello-aurora-dsql' >> $HOME/.bashrc
echo 'export HELLO_CODE_DIR=$HELLO_HOME/src/main/java/software/amazon/dsql' >> $HOME/.bashrc
echo 'export PATH=$PATH:$HELLO_HOME:$HOME/bin' >> $HOME/.bashrc
echo "export CODE_BUCKET=$BUCKET_NAME" >> $HOME/.bashrc

echo "Done"
