package utils

import (
	"context"
	"fmt"

	"github.com/zricethezav/gitleaks/v8/detect"
	"github.com/zricethezav/gitleaks/v8/sources"
)

func GitleaksScan(source string, outputfile string, size int) error {
	detector, err := detect.NewDetectorDefaultConfig()
	if err != nil {
		return fmt.Errorf("error creating gitleaks detector: %w", err)
	}
	detector.MaxArchiveDepth = 10
	detector.MaxDecodeDepth = 10
	sourceFiles := &sources.Files{
		Config:          &detector.Config,
		FollowSymlinks:  false,
		MaxFileSize:     detector.MaxTargetMegaBytes * 1_000_000,
		Path:            source,
		Sema:            detector.Sema,
		MaxArchiveDepth: detector.MaxArchiveDepth,
	}
	findings, err := detector.DetectSource(
		context.Background(),
		sourceFiles,
	)
	if err != nil {
		return fmt.Errorf("error detecting leaks: %w", err)
	}
	fmt.Println(findings)
	return nil
}

// func BuildCommand(source string, report_path string) *cobra.Command {
// 	cmd := &cobra.Command{
// 		Args: nil,
// 		Run:  DirectoryScan,
// 	}
// 	cmd.Flags().Int("max-target-megabytes", 0, "")
// 	cmd.Flags().Int("max-decode-depth", 0, "")
// 	cmd.Flags().Int("max-archive-depth", 0, "")
// 	cmd.Flags().Bool("no-color", false, "")
// 	cmd.Flags().String("config", "", "")
// 	cmd.Flags().Bool("verbose", false, "")
// 	cmd.Flags().Uint("redact", 0, "")
// 	cmd.Flags().Bool("ignore-gitleaks-allow", false, "")
// 	cmd.Flags().String("gitleaks-ignore-path", ".", "")
// 	cmd.Flags().String("baseline-path", "", "")
// 	cmd.Flags().StringSlice("enable-rule", []string{}, "")
// 	cmd.Flags().String("report-path", report_path, "")
// 	cmd.Flags().String("report-format", "json", "")
// 	cmd.Flags().String("report-template", "", "")

// 	return cmd
// }

// func DirectoryScan(cmd *cobra.Command, args []string) {
// 	source := args[0]
// 	outputfile := args[1]
// 	cfg := gitleaksCMD.Config(cmd)

// 	detector := gitleaksCMD.Detector(cmd, cfg, source)
// 	detector.FollowSymlinks = false
// 	detector.Sema = semgroup.NewGroup(context.Background(), int64(runtime.NumCPU()))
// 	findings, err := detector.DetectSource(
// 		context.Background(),
// 		&sources.Files{
// 			Config:          &cfg,
// 			FollowSymlinks:  detector.FollowSymlinks,
// 			MaxFileSize:     detector.MaxTargetMegaBytes * 1_000_000,
// 			Path:            source,
// 			Sema:            detector.Sema,
// 			MaxArchiveDepth: detector.MaxArchiveDepth,
// 		},
// 	)
// 	if err != nil {
// 		fmt.Println("Error detecting leaks:", err)
// 	}
// 	var file io.WriteCloser
// 	if file, err = os.Create(outputfile); err != nil {
// 		fmt.Println("Error creating report file:", err)
// 		return
// 	}
// 	if err = detector.Reporter.Write(file, findings); err != nil {
// 		fmt.Println("Error writing report:", err)
// 		return
// 	}

// }
