
#include "H264VideoFileSink.hh"
#include "OutputFile.hh"
#include "H264VideoRTPSource.hh"

////////// H264VideoFileSink //////////

H264VideoFileSink::H264VideoFileSink(UsageEnvironment& env, FILE* fid,char const* sPropParameterSetsStr,unsigned bufferSize, char const* perFrameFileNamePrefix)
: FileSink(env, fid, bufferSize, perFrameFileNamePrefix),fSPropParameterSetsStr(sPropParameterSetsStr), fHaveWrittenFirstFrame(False) 
{
}

H264VideoFileSink::~H264VideoFileSink() 
{
}

H264VideoFileSink*H264VideoFileSink::createNew(UsageEnvironment& env, char const* fileName,
											   char const* sPropParameterSetsStr,
											   unsigned bufferSize, Boolean oneFilePerFrame) 
{
	do {
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

		return new H264VideoFileSink(env, fid, sPropParameterSetsStr, bufferSize, perFrameFileNamePrefix);

	} while (0);

	return NULL;
}

void H264VideoFileSink::afterGettingFrame1(unsigned frameSize, struct timeval presentationTime) 
{
	unsigned char const start_code[4] = {0x00, 0x00, 0x00, 0x01};

	if (!fHaveWrittenFirstFrame) {
		// If we have PPS/SPS NAL units encoded in a "sprop parameter string", prepend these to the file:
		unsigned numSPropRecords;
		SPropRecord* sPropRecords = parseSPropParameterSets(fSPropParameterSetsStr, numSPropRecords);
		for (unsigned i = 0; i < numSPropRecords; ++i) {
			addData(start_code, 4, presentationTime);
			addData(sPropRecords[i].sPropBytes, sPropRecords[i].sPropLength, presentationTime);
		}
		delete[] sPropRecords;
		fHaveWrittenFirstFrame = True; // for next time
	}

	//Write the input data to the file, with the start code in front:
	addData(start_code, 4, presentationTime);

	//Call the parent class to complete the normal file write with the input data:
	FileSink::afterGettingFrame1(frameSize, presentationTime);

}
