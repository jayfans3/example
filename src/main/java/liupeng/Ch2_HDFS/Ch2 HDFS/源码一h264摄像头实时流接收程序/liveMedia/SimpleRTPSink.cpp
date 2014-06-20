
#include "SimpleRTPSink.hh"

SimpleRTPSink::SimpleRTPSink(UsageEnvironment& env, Groupsock* RTPgs,
							 unsigned char rtpPayloadFormat,
							 unsigned rtpTimestampFrequency,
							 char const* sdpMediaTypeString,
							 char const* rtpPayloadFormatName,
							 unsigned numChannels,
							 Boolean allowMultipleFramesPerPacket,
							 Boolean doNormalMBitRule)
							 : MultiFramedRTPSink(env, RTPgs, rtpPayloadFormat,
							 rtpTimestampFrequency, rtpPayloadFormatName,
							 numChannels),
							 fAllowMultipleFramesPerPacket(allowMultipleFramesPerPacket) 
{
	fSDPMediaTypeString = strDup(sdpMediaTypeString == NULL ? "unknown" : sdpMediaTypeString);
	fSetMBitOnLastFrames = strcmp(fSDPMediaTypeString, "video") == 0 && doNormalMBitRule;
}

SimpleRTPSink::~SimpleRTPSink() 
{
	delete[] (char*)fSDPMediaTypeString;
}

SimpleRTPSink*SimpleRTPSink::createNew(UsageEnvironment& env, Groupsock* RTPgs,
									   unsigned char rtpPayloadFormat,
									   unsigned rtpTimestampFrequency,
									   char const* sdpMediaTypeString,
									   char const* rtpPayloadFormatName,
									   unsigned numChannels,
									   Boolean allowMultipleFramesPerPacket,
									   Boolean doNormalMBitRule) 
{
	return new SimpleRTPSink(env, RTPgs,
		rtpPayloadFormat, rtpTimestampFrequency,
		sdpMediaTypeString, rtpPayloadFormatName,
		numChannels,
		allowMultipleFramesPerPacket,
		doNormalMBitRule);
}

void SimpleRTPSink::doSpecialFrameHandling( unsigned fragmentationOffset,
										    unsigned char* frameStart,
										    unsigned numBytesInFrame,
											struct timeval frameTimestamp,unsigned numRemainingBytes) 
{
	if (numRemainingBytes == 0) {
		// This packet contains the last (or only) fragment of the frame.
		// Set the RTP 'M' ('marker') bit, if appropriate:
		if (fSetMBitOnLastFrames) setMarkerBit();
	}

	// Important: Also call our base class's doSpecialFrameHandling(),
	// to set the packet's timestamp:
	MultiFramedRTPSink::doSpecialFrameHandling(fragmentationOffset,
		frameStart, numBytesInFrame,
		frameTimestamp,
		numRemainingBytes);
}

Boolean SimpleRTPSink::frameCanAppearAfterPacketStart(unsigned char const* /*frameStart*/,
													  unsigned /*numBytesInFrame*/) const 
{
	return fAllowMultipleFramesPerPacket;
}

char const* SimpleRTPSink::sdpMediaType() const 
{
	return fSDPMediaTypeString;
}
