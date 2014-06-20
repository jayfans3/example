
#if (defined(__WIN32__) || defined(_WIN32)) && !defined(_WIN32_WCE)
#include <io.h>
#include <fcntl.h>
#endif
#include "FileSink.hh"
#include "GroupsockHelper.hh"
#include "OutputFile.hh"

#include <time.h>  

////////// FileSink //////////

FileSink::FileSink(UsageEnvironment& env, FILE* fid, unsigned bufferSize,char const* perFrameFileNamePrefix)
: MediaSink(env), fOutFid(fid), fBufferSize(bufferSize) 
{
	fBuffer = new unsigned char[bufferSize];
	if (perFrameFileNamePrefix != NULL) {
		fPerFrameFileNamePrefix = strDup(perFrameFileNamePrefix);
		fPerFrameFileNameBuffer = new char[strlen(perFrameFileNamePrefix) + 100];
	} else {
		fPerFrameFileNamePrefix = NULL;
		fPerFrameFileNameBuffer = NULL;
	}
}

FileSink::~FileSink() 
{
  delete[] fPerFrameFileNameBuffer;
  delete[] fPerFrameFileNamePrefix;
  delete[] fBuffer;
  if (fOutFid != NULL) fclose(fOutFid);
}

FileSink* FileSink::createNew(UsageEnvironment& env, char const* fileName,unsigned bufferSize, Boolean oneFilePerFrame) 
{
	do 
	{
		FILE* fid;
		char const* perFrameFileNamePrefix;

		if (oneFilePerFrame) {
			// Create the fid for each frame
			fid = NULL;
			perFrameFileNamePrefix = fileName;
		} else {
			// Normal case: create the fid once
			fid = OpenOutputFile(env, fileName);
			if (fid == NULL) break;
			perFrameFileNamePrefix = NULL;
		}

		return new FileSink(env, fid, bufferSize, perFrameFileNamePrefix);

	} while (0);

	return NULL; 					  
}

Boolean FileSink::continuePlaying() 
{
	if (fSource == NULL) return False;

	fSource->getNextFrame(fBuffer, fBufferSize,afterGettingFrame, this,onSourceClosure, this);

	return True;
}

void FileSink::afterGettingFrame(void* clientData, unsigned frameSize,unsigned ,struct timeval presentationTime,unsigned) 
{
	FileSink* sink = (FileSink*)clientData;
	sink->afterGettingFrame1(frameSize, presentationTime);
}

int lastTime = 0;

void FileSink::addData(unsigned char const* data, unsigned dataSize,struct timeval presentationTime) 
{
	if (fPerFrameFileNameBuffer != NULL) {
		// Special case: Open a new file on-the-fly for this frame
		sprintf(fPerFrameFileNameBuffer, "%s-%lu.%06lu", fPerFrameFileNamePrefix,
			presentationTime.tv_sec, presentationTime.tv_usec);
		fOutFid = OpenOutputFile(envir(), fPerFrameFileNameBuffer);
	}
	// Write to our file:
#ifdef TEST_LOSS
	static unsigned const framesPerPacket = 10;
	static unsigned const frameCount = 0;
	static Boolean const packetIsLost;
	if ((frameCount++)%framesPerPacket == 0) {
		packetIsLost = (our_random()%10 == 0); // simulate 10% packet loss #####
	}
	if (!packetIsLost)
#endif
	if (fOutFid != NULL && data != NULL) {

		struct timeval thistTime;
		gettimeofday(&thistTime,NULL);
		
		if (thistTime.tv_sec!=lastTime && thistTime.tv_sec%10==0)//10s¼ä¸ô
		{
			lastTime = thistTime.tv_sec;

			time_t timep;   
			struct tm *p;   
			char filename[100];  
			
			fclose(fOutFid);
			time(&timep);  

			p=localtime(&timep);

			sprintf(filename,"C:\\msys\\1.0\\home\\admin\\ffmpeg\\data\\[%d-%02d-%02d]-[%02d-%02d-%02d].264",(1900+p->tm_year),( 1+p->tm_mon), p->tm_mday,p->tm_hour, p->tm_min, p->tm_sec);  

			printf("%s\n",filename);  

			fOutFid = fopen(filename,"wb"); 
		}
		fwrite(data, 1, dataSize, fOutFid);
	}
}

void FileSink::afterGettingFrame1(unsigned frameSize,struct timeval presentationTime) 
{
	addData(fBuffer, frameSize, presentationTime);

	if (fOutFid == NULL || fflush(fOutFid) == EOF) {
		// The output file has closed.Handle this the same way as if the input source had closed:
		onSourceClosure(this);
		stopPlaying();
		return;
	}
	
	if (fPerFrameFileNameBuffer != NULL) {
		if (fOutFid != NULL) { fclose(fOutFid); fOutFid = NULL; }
	}

	// Then try getting the next frame:
	continuePlaying();
}