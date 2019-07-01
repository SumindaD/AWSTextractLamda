import json
import boto3
import os
import urllib.parse
import io
from PIL import Image, ImageDraw

print('Loading function')

s3 = boto3.client('s3')
textract = boto3.client('textract')

def getTextractData(bucketName, documentKey):

    print('Loading getTextractData')
    # Call Amazon Textract
    response = textract.detect_document_text(
        Document={
            'S3Object': {
                'Bucket': bucketName,
                'Name': documentKey
            }
        })
    
    #Read Image Data from S3
    detectedText = ''
    image_object = s3.get_object(Bucket=bucketName,Key=documentKey)
    image_body_data = image_object['Body'].read()
    image = Image.open(io.BytesIO(image_body_data))
    width, height = image.size
    draw = ImageDraw.Draw(image)    


    # Print detected text and draw bounding box around detected texts
    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            detectedText += item['Text'] + '\n'
            x1 = item['Geometry']['BoundingBox']['Left'] * width
            y1 = item['Geometry']['BoundingBox']['Top'] * height
            x2 = x1 + item['Geometry']['BoundingBox']['Width'] * width
            y2 = y1 + item['Geometry']['BoundingBox']['Height'] * height
            draw.rectangle([x1, y1, x2, y2], outline="Black")
    
    #Get image byte array and save to S3
    imgByteArr = io.BytesIO()
    image.save(imgByteArr, format='PNG')
    s3.put_object(Bucket=bucketName,Key='Gen_' + documentKey,Body=imgByteArr.getvalue())
    
    return detectedText
    
def writeTextractToS3File(textractData, bucketName, createdS3Document):
    print('Loading writeTextractToS3File')
    generateFilePath = os.path.splitext(createdS3Document)[0] + '.txt'
    s3.put_object(Body=textractData, Bucket=bucketName, Key=generateFilePath)
    print('Generated ' + generateFilePath)

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        
        if('Gen_' not in key):
            detectedText = getTextractData(bucket, key)
            writeTextractToS3File(detectedText, bucket, key)
        
        return 'Processing Done!'

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
