import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Stepper,
  Step,
  StepLabel,
  Box,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  RadioGroup,
  FormControlLabel,
  Radio,
  Alert,
  Grid,
  Card,
  CardContent,
  Autocomplete,
} from '@mui/material';
import {
  Link as LinkIcon,
  Dataset,
  AccountTree,
} from '@mui/icons-material';
import { OntologyClass, Relationship } from '../../types/ontology';

interface LinkTypeWizardProps {
  open: boolean;
  onClose: () => void;
  classes: OntologyClass[];
  onSave: (relationship: Relationship) => void;
}

const steps = ['관계 유형 선택', '연결 설정', '이름 설정'];

export const LinkTypeWizard: React.FC<LinkTypeWizardProps> = ({
  open,
  onClose,
  classes,
  onSave,
}) => {
  const [activeStep, setActiveStep] = useState(0);
  const [linkType, setLinkType] = useState<'foreignKey' | 'dataset' | 'objectBacked'>('foreignKey');
  const [sourceClass, setSourceClass] = useState<string>('');
  const [targetClass, setTargetClass] = useState<string>('');
  const [linkName, setLinkName] = useState('');
  const [inverseLinkName, setInverseLinkName] = useState('');
  const [cardinality, setCardinality] = useState<string>('1:n');

  const handleNext = () => {
    setActiveStep((prevActiveStep) => prevActiveStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevActiveStep) => prevActiveStep - 1);
  };

  const handleReset = () => {
    setActiveStep(0);
    setLinkType('foreignKey');
    setSourceClass('');
    setTargetClass('');
    setLinkName('');
    setInverseLinkName('');
    setCardinality('1:n');
  };

  const handleFinish = () => {
    const relationship: Relationship = {
      predicate: linkName.toLowerCase().replace(/\s+/g, '_'),
      target: targetClass,
      label: linkName,
      cardinality: cardinality,
      inverse_predicate: inverseLinkName.toLowerCase().replace(/\s+/g, '_'),
      inverse_label: inverseLinkName,
    };
    onSave(relationship);
    handleReset();
    onClose();
  };

  const renderStepContent = (step: number) => {
    switch (step) {
      case 0:
        return (
          <Box>
            <Typography variant="h6" gutterBottom>
              관계 유형을 선택하세요
            </Typography>
            
            <RadioGroup
              value={linkType}
              onChange={(e) => setLinkType(e.target.value as any)}
            >
              <Card sx={{ mb: 2 }}>
                <CardContent>
                  <FormControlLabel
                    value="foreignKey"
                    control={<Radio />}
                    label={
                      <Box>
                        <Typography variant="subtitle1">
                          <LinkIcon sx={{ verticalAlign: 'middle', mr: 1 }} />
                          Foreign Key
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          일대일(1:1) 또는 다대일(n:1) 관계에 사용됩니다.
                          한 객체의 외래키를 통해 다른 객체를 참조합니다.
                        </Typography>
                      </Box>
                    }
                  />
                </CardContent>
              </Card>

              <Card sx={{ mb: 2 }}>
                <CardContent>
                  <FormControlLabel
                    value="dataset"
                    control={<Radio />}
                    label={
                      <Box>
                        <Typography variant="subtitle1">
                          <Dataset sx={{ verticalAlign: 'middle', mr: 1 }} />
                          Dataset
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          다대다(n:m) 관계에 사용됩니다.
                          별도의 조인 테이블로 두 객체 간 링크를 관리합니다.
                        </Typography>
                      </Box>
                    }
                  />
                </CardContent>
              </Card>

              <Card>
                <CardContent>
                  <FormControlLabel
                    value="objectBacked"
                    control={<Radio />}
                    label={
                      <Box>
                        <Typography variant="subtitle1">
                          <AccountTree sx={{ verticalAlign: 'middle', mr: 1 }} />
                          Object Type
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          객체 기반 링크로서, 링크 자체를 하나의 Object Type으로 모델링합니다.
                          링크에 추가 속성이 필요한 경우 사용합니다.
                        </Typography>
                      </Box>
                    }
                  />
                </CardContent>
              </Card>
            </RadioGroup>
          </Box>
        );

      case 1:
        return (
          <Box>
            <Typography variant="h6" gutterBottom>
              연결할 Object Type 선택
            </Typography>

            <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 2 }}>
              <Box>
                <Autocomplete
                  value={classes.find(c => c.id === sourceClass) || null}
                  onChange={(event, newValue) => {
                    setSourceClass(newValue?.id || '');
                  }}
                  options={classes}
                  getOptionLabel={(option) => option.label}
                  renderInput={(params) => (
                    <TextField {...params} label="Source Object Type" />
                  )}
                />
              </Box>

              <Box>
                <Autocomplete
                  value={classes.find(c => c.id === targetClass) || null}
                  onChange={(event, newValue) => {
                    setTargetClass(newValue?.id || '');
                  }}
                  options={classes}
                  getOptionLabel={(option) => option.label}
                  renderInput={(params) => (
                    <TextField {...params} label="Target Object Type" />
                  )}
                />
              </Box>

              <Box sx={{ gridColumn: '1 / -1' }}>
                <FormControl fullWidth>
                  <InputLabel>Cardinality</InputLabel>
                  <Select
                    value={cardinality}
                    onChange={(e) => setCardinality(e.target.value)}
                    label="Cardinality"
                  >
                    <MenuItem value="1:1">1:1 (One to One)</MenuItem>
                    <MenuItem value="1:n">1:n (One to Many)</MenuItem>
                    <MenuItem value="n:1">n:1 (Many to One)</MenuItem>
                    <MenuItem value="n:m">n:m (Many to Many)</MenuItem>
                  </Select>
                </FormControl>
              </Box>

              {linkType === 'dataset' && (
                <Box sx={{ gridColumn: '1 / -1' }}>
                  <Alert severity="info">
                    조인 테이블이 자동으로 생성됩니다.
                    필요한 경우 나중에 데이터소스 설정에서 변경할 수 있습니다.
                  </Alert>
                </Box>
              )}
            </Box>
          </Box>
        );

      case 2:
        return (
          <Box>
            <Typography variant="h6" gutterBottom>
              링크 이름 설정
            </Typography>

            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <Box>
                <Typography variant="body2" color="text.secondary" gutterBottom>
                  {sourceClass} → {targetClass} 방향
                </Typography>
                <TextField
                  fullWidth
                  label="Display Name"
                  value={linkName}
                  onChange={(e) => setLinkName(e.target.value)}
                  placeholder="예: Assigned Aircraft"
                  helperText="UI에 표시될 관계 이름"
                />
              </Box>

              <Box>
                <Typography variant="body2" color="text.secondary" gutterBottom>
                  {targetClass} → {sourceClass} 방향 (역방향)
                </Typography>
                <TextField
                  fullWidth
                  label="Inverse Display Name"
                  value={inverseLinkName}
                  onChange={(e) => setInverseLinkName(e.target.value)}
                  placeholder="예: Flights"
                  helperText="역방향 관계 이름"
                />
              </Box>

              <Box>
                <Alert severity="info">
                  API Name은 Display Name을 기반으로 자동 생성됩니다.
                  예: "Assigned Aircraft" → "assignedAircraft"
                </Alert>
              </Box>
            </Box>
          </Box>
        );

      default:
        return null;
    }
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      fullWidth
    >
      <DialogTitle>새 Link Type 만들기</DialogTitle>
      
      <DialogContent>
        <Box sx={{ width: '100%', mb: 3 }}>
          <Stepper activeStep={activeStep}>
            {steps.map((label) => (
              <Step key={label}>
                <StepLabel>{label}</StepLabel>
              </Step>
            ))}
          </Stepper>
        </Box>

        <Box sx={{ mt: 2, minHeight: 300 }}>
          {renderStepContent(activeStep)}
        </Box>
      </DialogContent>

      <DialogActions>
        <Button onClick={onClose}>취소</Button>
        <Box sx={{ flex: '1 1 auto' }} />
        <Button
          disabled={activeStep === 0}
          onClick={handleBack}
        >
          이전
        </Button>
        {activeStep === steps.length - 1 ? (
          <Button
            variant="contained"
            onClick={handleFinish}
            disabled={!linkName || !sourceClass || !targetClass}
          >
            완료
          </Button>
        ) : (
          <Button
            variant="contained"
            onClick={handleNext}
            disabled={
              (activeStep === 0 && !linkType) ||
              (activeStep === 1 && (!sourceClass || !targetClass))
            }
          >
            다음
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};